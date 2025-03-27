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

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        let mut iter = Self {
            current: None,
            next_sst_idx: 0,
            sstables,
        };
        // Only create iterator for first SST if we have any
        if !iter.sstables.is_empty() {
            iter.current = Some(SsTableIterator::create_and_seek_to_first(
                iter.sstables[0].clone(),
            )?);
            iter.next_sst_idx = 1;
        }
        Ok(iter)
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let mut iter = Self {
            current: None,
            next_sst_idx: 0,
            sstables,
        };

        // Find the first SST that may contain the key
        while iter.next_sst_idx < iter.sstables.len() {
            let sst = &iter.sstables[iter.next_sst_idx];
            if key <= sst.last_key().as_key_slice() {
                // This SST may contain the key, create an iterator
                iter.current = Some(SsTableIterator::create_and_seek_to_key(sst.clone(), key)?);
                iter.next_sst_idx += 1;
                break;
            }
            iter.next_sst_idx += 1;
        }

        Ok(iter)
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        self.current.as_ref().is_some_and(|iter| iter.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        if let Some(iter) = &mut self.current {
            iter.next()?;

            // If current iterator is exhausted, try next SST
            if !iter.is_valid() && self.next_sst_idx < self.sstables.len() {
                self.current = Some(SsTableIterator::create_and_seek_to_first(
                    self.sstables[self.next_sst_idx].clone(),
                )?);
                self.next_sst_idx += 1;
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        // We only maintain one active iterator at a time
        if self.current.is_some() { 1 } else { 0 }
    }
}
