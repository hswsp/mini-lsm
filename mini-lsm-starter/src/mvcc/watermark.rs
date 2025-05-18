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

use std::collections::BTreeMap;

pub struct Watermark {
    /// Map from timestamp to number of active transactions at that time
    readers: BTreeMap<u64, usize>,
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    /// Adds a reader at the given timestamp
    pub fn add_reader(&mut self, ts: u64) {
        *self.readers.entry(ts).or_insert(0) += 1;
    }

    /// Removes a reader at the given timestamp
    pub fn remove_reader(&mut self, ts: u64) {
        if let Some(count) = self.readers.get_mut(&ts) {
            *count -= 1;
            if *count == 0 {
                // Remove entry if no more readers at this timestamp
                self.readers.remove(&ts);
            }
        }
    }

    pub fn num_retained_snapshots(&self) -> usize {
        self.readers.len()
    }

    /// Returns the lowest read timestamp among active transactions,
    /// or None if there are no active transactions
    pub fn watermark(&self) -> Option<u64> {
        // First key in BTreeMap is the lowest timestamp
        self.readers.keys().next().copied()
    }

    /// Returns a debug view of current readers
    pub fn debug_readers(&self) -> Vec<(u64, usize)> {
        self.readers
            .iter()
            .map(|(ts, count)| (*ts, *count))
            .collect()
    }
}

impl Default for Watermark {
    fn default() -> Self {
        Self::new()
    }
}
