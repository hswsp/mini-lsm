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

use std::ops::Bound;

use anyhow::{Result, bail};
use bytes::Bytes;

use crate::{
    iterators::{
        StorageIterator, concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the course for multiple times.
/// type LsmIteratorInner = MergeIterator<MemTableIterator>;
/// type LsmIteratorInner =
///      TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;
///
// Update the type definition to use nested TwoMergeIterator
type LsmIteratorInner = TwoMergeIterator<
    // First part: merge memtables and L0 SSTs
    TwoMergeIterator<
        MergeIterator<MemTableIterator>, // Memtables iterator
        MergeIterator<SsTableIterator>,  // L0 SSTs iterator
    >,
    // Second part: L1+ iterator (which uses concat iterator internally)
    MergeIterator<SstConcatIterator>, // L1+ SSTs iterator using concat
>;

// 这里end_bound被check了， begin_bound没有被check，取决去初始的inner。
pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
    is_valid: bool,
    read_ts: u64,
    prev_key: Vec<u8>,
}

impl LsmIterator {
    pub(crate) fn new(
        iter: LsmIteratorInner,
        end_bound: impl Into<Bound<Bytes>>,
        read_ts: u64,
    ) -> Result<Self> {
        let mut iter = Self {
            is_valid: iter.is_valid(),
            inner: iter,
            end_bound: end_bound.into(),
            read_ts,
            prev_key: Vec::new(),
        };
        let debug_enabled = std::env::var("RUST_BACKTRACE")
            .map(|val| val == "1")
            .unwrap_or(false);
        if debug_enabled {
            println!("======== init LsmIterator ========");
        }
        iter.move_to_key()?;
        if debug_enabled {
            println!("======== init LsmIterator move_to_key done ========");
        }
        Ok(iter)
    }

    fn check_end_bound(&self) -> bool {
        match &self.end_bound.as_ref() {
            Bound::Included(bound) => self.inner.key().key_ref() <= bound.as_ref(),
            Bound::Excluded(bound) => self.inner.key().key_ref() < bound.as_ref(),
            Bound::Unbounded => true,
        }
    }

    /// Moves the underlying iterator to the next position
    fn next_inner(&mut self) -> Result<()> {
        // Advance underlying iterator
        self.inner.next()?;

        if !self.is_valid() {
            self.is_valid = false;
        }
        Ok(())
    }

    /// Moves to the next valid key, skipping:
    /// 1. Keys with timestamp > read_ts
    /// 2. Older versions of the same key
    /// 3. Tombstone (deleted) keys
    fn move_to_key(&mut self) -> Result<()> {
        let debug_enabled = std::env::var("RUST_BACKTRACE")
            .map(|val| val == "1")
            .unwrap_or(false);
        if debug_enabled {
            println!("[LsmIterator] current read_ts: {}", self.read_ts);
        }

        while self.is_valid() {
            // Skip older versions of the same key
            if self.key() == self.prev_key.as_slice() {
                self.inner.next()?;
                continue;
            }
            // Record current key before moving
            self.prev_key = self.key().to_vec();

            if debug_enabled {
                println!(
                    "[LsmIterator] prev_key='{}', current_key='{}' (ts={}), value='{}'",
                    String::from_utf8_lossy(&self.prev_key),
                    String::from_utf8_lossy(self.inner.key().key_ref()),
                    self.inner.key().ts(),
                    String::from_utf8_lossy(self.inner.value()),
                );
            }

            // Skip keys with timestamp greater than read_ts
            while self.is_valid()
                && self.inner.key().key_ref() == self.prev_key
                && self.inner.key().ts() > self.read_ts
            {
                self.inner.next()?;
            }

            if !self.inner.is_valid() {
                break;
            }

            // If all put ts > read_ts, will go this line to the next key
            if self.inner.key().key_ref() != self.prev_key {
                continue;
            }

            // Skip tombstones (deleted keys)
            if !self.inner.value().is_empty() {
                // Found a valid key
                break;
            }
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.inner.is_valid() && self.check_end_bound()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        if !self.is_valid() {
            return Ok(());
        }
        self.next_inner()?;
        self.move_to_key()?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        let mut fused = Self {
            iter,
            has_errored: false,
        };

        // Skip any deleted keys (empty values) at initialization
        while fused.iter.is_valid() && fused.iter.value().is_empty() {
            if fused.iter.next().is_err() {
                fused.has_errored = true;
                break;
            }
        }

        fused
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if self.has_errored {
            panic!("called key() on invalid iterator");
        }
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        if self.has_errored {
            panic!("called value() on invalid iterator");
        }
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("iterator already errored");
        }

        // Continue calling next() while:
        // 1. The iterator is valid
        // 2. The current value is empty (indicates deletion)
        while self.iter.is_valid() {
            match self.iter.next() {
                Ok(_) => {
                    // 如果当前值是空的（删除标记），继续调用 next
                    if !self.iter.is_valid() || !self.iter.value().is_empty() {
                        break;
                    }
                }
                Err(e) => {
                    self.has_errored = true;
                    return Err(e);
                }
            }
        }

        // !is_valid() do nothing
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
