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

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::with_capacity(block_size),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        // Calculate the size needed for this entry
        let entry_size = 2 + key.raw_ref().len() + 2 + value.len(); // key_len + key + value_len + value

        // Calculate the total size of current block
        let total_size = self.data.len() + self.offsets.len() * 2;

        // If this is not the first entry, Check if adding this entry would exceed block size
        if !self.is_empty() && total_size + entry_size > self.block_size {
            return false;
        }

        // If this is the first key, store it
        if self.is_empty() {
            self.first_key = key.to_key_vec();
        }

        // Store the offset of this entry
        self.offsets.push(self.data.len() as u16);

        // Write key length and key
        self.data
            .extend_from_slice(&(key.raw_ref().len() as u16).to_le_bytes());
        self.data.extend_from_slice(key.raw_ref());

        // Write value length and value
        self.data
            .extend_from_slice(&(value.len() as u16).to_le_bytes());
        self.data.extend_from_slice(value);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
