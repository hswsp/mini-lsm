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

use bytes::Bytes;
use std::mem;
use std::path::Path;
use std::sync::Arc; // Add this import

use anyhow::Result;

use super::{BlockMeta, FileObject, KeyBytes, SsTable};
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        SsTableBuilder {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        // If this is the first key, create a new meta
        if self.first_key.is_empty() {
            self.first_key = key.raw_ref().to_vec();
            self.meta.push(BlockMeta {
                offset: 0,
                first_key: KeyBytes::from_bytes(Bytes::copy_from_slice(key.raw_ref())),
                last_key: KeyBytes::from_bytes(Bytes::copy_from_slice(key.raw_ref())),
            });
        }

        if !self.builder.add(key, value) {
            // Try to add the entry to current block
            self.seal_block();
            // Create a new meta for the new block
            self.meta.push(BlockMeta {
                offset: self.data.len(),
                first_key: KeyBytes::from_bytes(Bytes::copy_from_slice(key.raw_ref())),
                last_key: KeyBytes::from_bytes(Bytes::copy_from_slice(key.raw_ref())),
            });
            assert!(self.builder.add(key, value));
        }

        // Update last key
        self.last_key = key.raw_ref().to_vec();

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
        if let Some(current_meta) = self.meta.last_mut() {
            current_meta.last_key = KeyBytes::from_bytes(Bytes::copy_from_slice(&self.last_key));
        }

        self.data.extend_from_slice(&block.encode());
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

        // 准备数据
        let mut buf = self.data;

        // 记录元数据起始位置
        let meta_offset = buf.len();

        // 添加sstable元数据
        BlockMeta::encode_block_meta(&self.meta, &mut buf);

        // 添加元数据偏移量
        buf.extend_from_slice(&(meta_offset as u32).to_le_bytes());

        // 创建文件
        let file = FileObject::create(path.as_ref(), buf)?;

        // 构建SSTable
        Ok(SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset: meta_offset,
            id,
            block_cache,
            first_key: KeyBytes::from_bytes(Bytes::copy_from_slice(&self.first_key)),
            last_key: KeyBytes::from_bytes(Bytes::copy_from_slice(&self.last_key)),
            bloom: None,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
