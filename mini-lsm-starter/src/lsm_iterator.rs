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
        StorageIterator, merge_iterator::MergeIterator, two_merge_iterator::TwoMergeIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the course for multiple times.
// type LsmIteratorInner = MergeIterator<MemTableIterator>;
type LsmIteratorInner =
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, end_bound: impl Into<Bound<Bytes>>) -> Result<Self> {
        Ok(Self {
            inner: iter,
            end_bound: end_bound.into(),
        })
    }

    fn check_end_bound(&self) -> bool {
        match &self.end_bound {
            Bound::Included(bound) => self.inner.key().raw_ref() <= bound.as_ref(),
            Bound::Excluded(bound) => self.inner.key().raw_ref() < bound.as_ref(),
            Bound::Unbounded => true,
        }
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.inner.is_valid() && self.check_end_bound()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        // 推进底层迭代器
        self.inner.next()?;

        // 跳过删除标记（空值）
        while self.is_valid() && self.inner.value().is_empty() {
            self.inner.next()?;
        }

        Ok(())
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
}
