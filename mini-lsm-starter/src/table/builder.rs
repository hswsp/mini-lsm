// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use farmhash::fingerprint32;
use std::mem;
use std::path::Path;
use std::sync::Arc; // Add this import

use anyhow::Result;

use super::{BlockMeta, FileObject, SsTable, bloom::Bloom};
use crate::{
    block::BlockBuilder,
    key::{KeySlice, KeyVec},
    lsm_storage::BlockCache,
};
use crc32fast::Hasher;
/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyVec,
    last_key: KeyVec,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
    max_ts: u64,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        SsTableBuilder {
            builder: BlockBuilder::new(block_size),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
            max_ts: 0, // Initialize max_ts
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        // Update max timestamp
        self.max_ts = std::cmp::max(self.max_ts, key.ts());
        // Compute hash for bloom filter
        self.key_hashes.push(fingerprint32(key.key_ref()));

        // If this is the first key, create a new meta
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
            self.meta.push(BlockMeta {
                offset: 0,
                first_key: key.to_key_vec().into_key_bytes(),
                last_key: key.to_key_vec().into_key_bytes(),
            });
        }

        if !self.builder.add(key, value) {
            // Try to add the entry to current block
            self.seal_block();
            // Create a new meta for the new block
            self.meta.push(BlockMeta {
                offset: self.data.len(),
                first_key: key.to_key_vec().into_key_bytes(),
                last_key: key.to_key_vec().into_key_bytes(),
            });
            assert!(self.builder.add(key, value));
        }

        // Update last key
        self.last_key = key.to_key_vec();

        // Print first and last keys as both hex and UTF-8 strings
        // println!(
        //     "Current state: first_key: {} ({:02?}), last_key: {} ({:02?})",
        //     String::from_utf8_lossy(&self.first_key),
        //     self.first_key,
        //     String::from_utf8_lossy(&self.last_key),
        //     self.last_key
        // );
    }

    /// Seals the current block and prepares for a new block.
    fn seal_block(&mut self) {
        let old_builder = mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let block = old_builder.build();

        // Update the last key of current meta before sealing
        if let Some(last_meta) = self.meta.last_mut() {
            last_meta.last_key = self.last_key.clone().into_key_bytes();
        }

        // Get block data
        let block_data = block.encode();

        // Calculate checksum for the block
        let mut hasher = Hasher::new();
        hasher.update(&block_data);
        let checksum = hasher.finalize();

        // Write block data and its checksum
        self.data.extend_from_slice(&block_data);
        self.data.extend_from_slice(&checksum.to_le_bytes());
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        // Return the current data size plus any data in the current builder
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // 如果最后一个block还未完成，添加到buf中
        if !self.builder.is_empty() {
            self.seal_block();
        }
        
        {
            // Debug print SSTable metadata
            println!("\n=== SSTable Build Info ===");
            println!("SSTable ID: {}", id);
            println!("Max Timestamp: {}", self.max_ts);
            println!("First Key: '{}' (ts: {})", 
                String::from_utf8_lossy(self.first_key.key_ref()),
                self.first_key.ts()
            );
            println!("Last Key: '{}' (ts: {})", 
                String::from_utf8_lossy(self.last_key.key_ref()),
                self.last_key.ts()
            );
            println!("Number of Blocks: {}", self.meta.len());
            println!("Total Data Size: {} bytes", self.data.len());
            println!("========================\n");
        }
        
        // 准备数据
        let mut buf = self.data;

        // 记录元数据起始位置
        let meta_offset = buf.len();
        // Encode block meta with max_ts
        BlockMeta::encode_block_meta(&self.meta, self.max_ts, &mut buf);
        // 添加元数据偏移量
        buf.extend_from_slice(&(meta_offset as u32).to_le_bytes());

        // Build and write bloom filter
        let bits_per_key = Bloom::bloom_bits_per_key(self.key_hashes.len() * 8, 0.1);
        let bloom = Bloom::build_from_key_hashes(&self.key_hashes, bits_per_key);
        let bloom_offset = buf.len();
        bloom.encode(&mut buf);
        buf.extend_from_slice(&(bloom_offset as u32).to_le_bytes());

        // 创建文件
        let file = FileObject::create(path.as_ref(), buf)?;

        // 构建SSTable
        Ok(SsTable {
            id,
            file,
            first_key: self.first_key.into_key_bytes(),
            last_key: self.last_key.into_key_bytes(),
            block_meta: self.meta,
            block_meta_offset: meta_offset,
            block_cache,
            bloom: Some(bloom),
            max_ts: self.max_ts,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
