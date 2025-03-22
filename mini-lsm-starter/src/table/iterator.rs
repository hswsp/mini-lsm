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

use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
/// you should load data on demand.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

// Find the appropriate block index for the given key in an SSTable.
/// Returns the block index where the key should be located.
fn find_block_idx_for_key(table: Arc<SsTable>, key: KeySlice) -> usize {
    let block_idx = table.find_block_idx(key);

    // Handle case where key is smaller than all blocks
    if block_idx >= table.block_meta.len() {
        return 0;
    }

    let block_meta = &table.block_meta[block_idx];

    // If key is larger than block's last key, move to next block
    match block_meta.last_key.as_key_slice().cmp(&key) {
        std::cmp::Ordering::Less => {
            // Clamp the next block index to valid range
            (block_idx + 1).min(table.block_meta.len() - 1)
        }
        _ => block_idx,
    }
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block = table.read_block_cached(0)?;
        let blk_iter = BlockIterator::create_and_seek_to_first(block);

        Ok(SsTableIterator {
            blk_iter,
            blk_idx: 0,
            table,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.blk_idx = 0;
        let block = self.table.read_block_cached(self.blk_idx)?;
        self.blk_iter = BlockIterator::create_and_seek_to_first(block);
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let blk_idx = find_block_idx_for_key(table.clone(), key);
        let block = table.read_block_cached(blk_idx)?;
        let blk_iter = BlockIterator::create_and_seek_to_key(block, key);

        Ok(SsTableIterator {
            blk_iter,
            blk_idx,
            table,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    /// only using the first key of each block to do the binary search.
    /// Also leverage the last key metadata to directly position to a correct block
    /// It is possible that the key does not exist in the LSM tree so that the block iterator will be invalid immediately after a seek
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        self.blk_idx = find_block_idx_for_key(self.table.clone(), key);
        let block = self.table.read_block_cached(self.blk_idx)?;
        self.blk_iter = BlockIterator::create_and_seek_to_key(block, key);
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.blk_iter.is_valid() {
            self.blk_idx += 1;
            if self.blk_idx < self.table.num_of_blocks() {
                let block = self.table.read_block_cached(self.blk_idx)?;
                self.blk_iter = BlockIterator::create_and_seek_to_first(block);
            }
        }
        Ok(())
    }
}
