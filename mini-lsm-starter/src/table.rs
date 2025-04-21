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

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::Buf;
use farmhash::fingerprint32;
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        for meta in block_meta {
            buf.extend_from_slice(&(meta.offset as u64).to_le_bytes());
            buf.extend_from_slice(&(meta.first_key.len() as u64).to_le_bytes());
            buf.extend_from_slice(meta.first_key.raw_ref());
            buf.extend_from_slice(&(meta.last_key.len() as u64).to_le_bytes());
            buf.extend_from_slice(meta.last_key.raw_ref());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(buf: impl Buf) -> Vec<BlockMeta> {
        let mut block_meta = Vec::new();
        let mut buf = buf;
        while buf.has_remaining() {
            let offset = buf.get_u64_le() as usize;
            let first_key_len = buf.get_u64_le() as usize;
            let first_key = KeyBytes::from_bytes(buf.copy_to_bytes(first_key_len));
            let last_key_len = buf.get_u64_le() as usize;
            let last_key = KeyBytes::from_bytes(buf.copy_to_bytes(last_key_len));
            block_meta.push(BlockMeta {
                offset,
                first_key,
                last_key,
            });
        }
        block_meta
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
/// encoding of SST is like:
/// -----------------------------------------------------------------------------------------------------
/// |         Block Section         |                            Meta Section                           |
/// -----------------------------------------------------------------------------------------------------
/// | data block | ... | data block | metadata | meta block offset | bloom filter | bloom filter offset |
/// |                               |  varlen  |         u32       |    varlen    |        u32          |
/// -----------------------------------------------------------------------------------------------------
///                        block_meta_offset    meta_offset_pos      bloom_offset   bloom_offset_pos
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let file_size = file.size();

        // Read bloom filter offset
        let bloom_offset_pos = file_size - 4;
        let bloom_offset_data = file.read(bloom_offset_pos, 4)?;
        let bloom_offset = u32::from_le_bytes(bloom_offset_data.try_into().unwrap()) as usize;

        // Read meta offset
        let meta_offset_pos = bloom_offset - 4;
        let meta_offset = file.read(meta_offset_pos as u64, 4)?;
        let block_meta_offset = u32::from_le_bytes(meta_offset.try_into().unwrap()) as usize;

        // Read block metadata
        let block_meta_data = file.read(
            block_meta_offset as u64,
            (meta_offset_pos - block_meta_offset) as u64,
        )?;
        let block_meta = BlockMeta::decode_block_meta(block_meta_data.as_slice());

        // Read bloom filter
        let bloom_data = file.read(
            bloom_offset as u64,
            bloom_offset_pos - (bloom_offset as u64),
        )?;
        let bloom = Some(Bloom::decode(&bloom_data)?);

        let first_key = block_meta.first().unwrap().first_key.clone();
        let last_key = block_meta.last().unwrap().last_key.clone();
        Ok(Self {
            file,
            block_meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom,
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let block_meta = &self.block_meta[block_idx];
        let block_size = if block_idx + 1 < self.block_meta.len() {
            self.block_meta[block_idx + 1].offset - block_meta.offset
        } else {
            // For the last block, read until the meta section
            self.block_meta_offset - block_meta.offset
        };

        let block_data = self
            .file
            .read(block_meta.offset as u64, block_size as u64)?;
        Ok(Arc::new(Block::decode(block_data.as_slice())))
    }

    /// Read a block from disk, with block cache. (Day 4)
    /// Blocks are cached by (sst_id, block_id) as the cache key
    /// You may use try_get_with to get the block from cache if it hits the cache / populate the cache if it misses the cache.
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(cache) = &self.block_cache {
            let cache_key = (self.id, block_idx);
            match cache.try_get_with(cache_key, || self.read_block(block_idx)) {
                Ok(block) => Ok(block),
                Err(e) => Err(anyhow::Error::msg(e)),
            }
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        self.block_meta
            .binary_search_by(|meta| meta.first_key.as_key_slice().cmp(&key))
            .unwrap_or_else(|x| if x == 0 { self.block_meta.len() } else { x - 1 })
        // finds the largest index where the key could be inserted
        // 如果 x == 0, 表示key比所有的都要小，用特殊值 self.block_meta.len()表示
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }

    // Add may_contain method for bloom filter checks
    pub fn may_contain(&self, key: &[u8]) -> bool {
        match &self.bloom {
            Some(bloom) => bloom.may_contain(fingerprint32(key)),
            None => true,
        }
    }
}
