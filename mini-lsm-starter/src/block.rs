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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Bytes;
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
/// num_of_elements not stored in the block, but can be inferred from offsets.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    /// block encoding format
    /// --------------------------------------------------------------------------------------------------------------------
    /// |             Data Section             |              Offset Section                            |      Extra      |
    /// --------------------------------------------------------------------------------------------------------------------
    /// | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 (2B) | Offset #2 (2B) | ... | Offset #N (2B) | num_of_elements |
    /// --------------------------------------------------------------------------------------------------------------------
    /// Each entry is a key-value pair:
    /// -----------------------------------------------------------------------
    /// |                           Entry #1                            | ... |
    /// -----------------------------------------------------------------------
    /// | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
    /// -----------------------------------------------------------------------
    ///
    /// After adding timestamp, Entry should be:
    /// | key_overlap_len (u16) | remaining_key_len (u16) | key (remaining_key_len) | timestamp (u64) |
    pub fn encode(&self) -> Bytes {
        let mut buf = Vec::with_capacity(
            self.data.len() + // Data Section
            self.offsets.len() * 2 + // Offset Section (2B each)
            2, // num_of_elements (2B)
        );

        // 1. Write Data Section
        buf.extend_from_slice(&self.data);

        // 2. Write Offset Section
        for offset in &self.offsets {
            buf.extend_from_slice(&offset.to_le_bytes());
        }

        // 3. Write number of elements
        buf.extend_from_slice(&(self.offsets.len() as u16).to_le_bytes());

        // Convert to Bytes
        Bytes::from(buf)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    /// Format of each entry:
    /// | key_overlap_len (2B) | remaining_key_len (2B) | key (remaining_key_len) | timestamp (8B) | value_len (2B) | value |
    pub fn decode(data: &[u8]) -> Self {
        // Read number of elements (last 2 bytes)
        let num_elements = u16::from_le_bytes(data[data.len() - 2..].try_into().unwrap()) as usize;

        // Calculate sections
        let offsets_section_size = num_elements * 2; // 2 bytes per offset
        let offsets_start = data.len() - 2 - offsets_section_size;

        // Extract offsets
        let mut offsets = Vec::with_capacity(num_elements);
        for i in 0..num_elements {
            let offset_bytes = &data[offsets_start + i * 2..offsets_start + (i + 1) * 2];
            offsets.push(u16::from_le_bytes(offset_bytes.try_into().unwrap()));
        }

        // Extract data
        let data_section = data[..offsets_start].to_vec();

        Block {
            data: data_section,
            offsets,
        }
    }
}
