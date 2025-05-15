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

use std::{
    collections::HashSet,
    ops::Bound,
    sync::{Arc, atomic::AtomicBool},
};

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{StorageIterator, two_merge_iterator::TwoMergeIterator},
    key::map_bound_to_bytes,
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::LsmStorageInner,
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        // First check local storage for any uncommitted changes
        if let Some(value) = self.local_storage.get(key) {
            return Ok(Some(value.value().clone()));
        }

        // If not in local storage, query storage engine with read timestamp
        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        // Convert bounds to owned bytes for storage operations
        let lower_bytes = map_bound_to_bytes(lower);
        let upper_bytes = map_bound_to_bytes(upper);

        // Create iterator over local storage modifications
        let local_iter = TxnLocalIteratorBuilder {
            map: Arc::clone(&self.local_storage),
            iter_builder: |map| map.range((lower_bytes, upper_bytes)),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();

        // Get iterator from storage engine with read timestamp
        println!("[Transaction] current read_ts: {}", self.read_ts);
        let storage_iter = self.inner.scan_with_ts(lower, upper, self.read_ts)?;

        // Merge local and storage iterators
        let merged_iter = TwoMergeIterator::create(local_iter, storage_iter)?;

        // Create transaction iterator
        TxnIterator::create(Arc::clone(self), merged_iter)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        unimplemented!()
    }

    pub fn delete(&self, key: &[u8]) {
        unimplemented!()
    }

    pub fn commit(&self) -> Result<()> {
        unimplemented!()
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {}
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

/// 这个实现依赖于：
///     TwoMergeIterator 正确处理版本合并
///     TxnLocalIterator 提供事务本地修改的视图
///     LsmIterator 处理存储引擎的多版本数据
/// 通过这样的分层设计，我们实现了：
///     事务隔离性 - 通过合并本地修改
///     多版本并发控制 - 通过时间戳过滤
///     一致性视图 - 通过保持事务存活直到迭代器关闭
#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl TxnLocalIterator {
    pub fn create_empty(map: Arc<SkipMap<Bytes, Bytes>>) -> Self {
        let empty_bounds = (Bound::Unbounded, Bound::Unbounded);
        TxnLocalIteratorBuilder {
            map: map.clone(),
            iter_builder: |map| map.range(empty_bounds),
            item: (Bytes::new(), Bytes::new()),
        }
        .build()
    }
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        // Return current value from item
        self.borrow_item().1.as_ref()
    }

    fn key(&self) -> &[u8] {
        // Return current key from item
        self.borrow_item().0.as_ref()
    }

    fn is_valid(&self) -> bool {
        // Valid if we have a non-empty key in current item
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        // With self_referencing, we need to use with_mut to modify fields
        self.with_mut(|fields| {
            // Get next item from skipmap iterator
            if let Some(entry) = fields.iter.next() {
                // Update current item with next key-value pair using tuple indexing
                fields.item.0 = entry.key().clone();
                fields.item.1 = entry.value().clone();
            } else {
                // No more items, set empty key/value to mark invalid
                fields.item.0 = Bytes::new();
                fields.item.1 = Bytes::new();
            }
            Ok(())
        })
    }
}

pub struct TxnIterator {
    _txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        Ok(Self { _txn: txn, iter })
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        // Simply delegate to the underlying iterator
        // The TwoMergeIterator will handle merging results from local storage and LSM storage
        self.iter.next()
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
