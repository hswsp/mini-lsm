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

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        // Initialize first_key before seeking
        if !iter.block.offsets.is_empty() {
            iter.first_key = iter.get_first_key_at_offset(iter.block.offsets[0] as usize);
        }
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.idx = 0;
        if self.idx < self.block.offsets.len() {
            self.first_key = self.get_key(self.idx);
            self.key = self.first_key.clone();
            self.value_range = self.get_value_range(self.idx);
        } else {
            self.key.clear();
        }
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        if self.idx < self.block.offsets.len() {
            self.key = self.get_key(self.idx);
            self.value_range = self.get_value_range(self.idx);
        } else {
            self.key.clear();
        }
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let pos = self.find_key_position(key);
        if pos < self.block.offsets.len() {
            self.idx = pos;
            self.key = self.get_key(self.idx);
            self.value_range = self.get_value_range(self.idx);
        } else {
            self.key.clear();
        }
    }

    /// Helper function to get the key at a given index.
    fn get_key(&self, idx: usize) -> KeyVec {
        let offset = self.block.offsets[idx] as usize;
        if idx == 0 {
            // For first key, read directly without prefix compression
            self.get_first_key_at_offset(offset)
        } else {
            self.get_key_at_offset(offset)
        }
    }

    /// Helper function to get the first key at offset without prefix compression
    fn get_first_key_at_offset(&self, offset: usize) -> KeyVec {
        // For first key, ignore key_overlap_len as it should be 0
        let rest_key_len =
            u16::from_le_bytes(self.block.data[offset + 2..offset + 4].try_into().unwrap())
                as usize;

        // Read the complete key
        let key_data = &self.block.data[offset + 4..offset + 4 + rest_key_len];
        KeyVec::from_vec(key_data.to_vec())
    }

    /// Helper function to get the key at a given offset
    fn get_key_at_offset(&self, offset: usize) -> KeyVec {
        // Read key_overlap_len
        let key_overlap_len =
            u16::from_le_bytes(self.block.data[offset..offset + 2].try_into().unwrap()) as usize;

        // Read rest_key_len
        let rest_key_len =
            u16::from_le_bytes(self.block.data[offset + 2..offset + 4].try_into().unwrap())
                as usize;

        // Construct full key using overlap
        let mut full_key = Vec::with_capacity(key_overlap_len + rest_key_len);

        // Add overlapping part from first key
        if key_overlap_len > 0 {
            full_key.extend_from_slice(&self.first_key.raw_ref()[..key_overlap_len]);
        }

        // Add rest of the key
        full_key.extend_from_slice(&self.block.data[offset + 4..offset + 4 + rest_key_len]);

        KeyVec::from_vec(full_key)
    }

    /// Helper function to get the value range at a given index.
    fn get_value_range(&self, idx: usize) -> (usize, usize) {
        let offset = self.block.offsets[idx] as usize;

        // Read key_overlap_len and rest_key_len
        let key_overlap_len =
            u16::from_le_bytes(self.block.data[offset..offset + 2].try_into().unwrap()) as usize;
        let rest_key_len =
            u16::from_le_bytes(self.block.data[offset + 2..offset + 4].try_into().unwrap())
                as usize;

        // Calculate value position
        let value_offset = offset + 4 + rest_key_len;
        let value_len = u16::from_le_bytes(
            self.block.data[value_offset..value_offset + 2]
                .try_into()
                .unwrap(),
        ) as usize;
        (value_offset + 2, value_offset + 2 + value_len)
    }

    /// Helper function to find the position of the first key that is >= `key`.
    fn find_key_position(&self, key: KeySlice) -> usize {
        self.block
            .offsets
            .binary_search_by(|&offset| {
                let probe = if offset == self.block.offsets[0] {
                    // If it's the first offset, use get_first_key_at_offset
                    self.get_first_key_at_offset(offset as usize)
                } else {
                    // For other offsets, use get_key_at_offset
                    self.get_key_at_offset(offset as usize)
                };
                probe.as_key_slice().cmp(&key)
            })
            .unwrap_or_else(|x| x) // Correctly returns the insertion point if the key is not found
    }
}
