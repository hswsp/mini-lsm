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
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
};

use super::CommittedTxnData;

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    fn check_not_committed(&self) -> Result<()> {
        if self.committed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(anyhow::anyhow!("transaction already committed"));
        }
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.check_not_committed()?;

        // Add key to read set if serializable isolation is enabled
        if let Some(key_hashes) = &self.key_hashes {
            let hash = farmhash::hash32(key);
            let mut guard = key_hashes.lock();
            guard.1.insert(hash); // Insert into read set (second element of tuple)
        }

        // First check local storage for any uncommitted changes
        if let Some(value) = self.local_storage.get(key) {
            let value = value.value();
            // If value is empty, it's a deletion marker - return None
            if value.is_empty() {
                return Ok(None);
            }
            // Otherwise return the local value
            return Ok(Some(value.clone()));
        }

        // If not in local storage, query storage engine with read timestamp
        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        self.check_not_committed()?;

        // Create iterator over local storage modifications
        let local_iter =
            TxnLocalIterator::create_empty(Arc::clone(&self.local_storage), lower, upper);

        // Get iterator from storage engine with read timestamp
        println!("[Transaction] current read_ts: {}", self.read_ts);
        let storage_iter = self.inner.scan_with_ts(lower, upper, self.read_ts)?;

        // Merge local and storage iterators
        let merged_iter = TwoMergeIterator::create(local_iter, storage_iter)?;

        // Create transaction iterator
        TxnIterator::create(Arc::clone(self), merged_iter)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        let _ = self.check_not_committed();

        // Add key to write set if serializable isolation is enabled
        if let Some(key_hashes) = &self.key_hashes {
            let hash = farmhash::hash32(key);
            let mut guard = key_hashes.lock();
            guard.0.insert(hash); // Insert into write set (first element of tuple)
        }

        // Insert the key-value pair into local storage
        // Convert input slices to Bytes for storage
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    pub fn delete(&self, key: &[u8]) {
        // Convert empty Bytes to &[u8] slice by using as_ref()
        self.put(key, Bytes::new().as_ref());
    }

    pub fn commit(&self) -> Result<()> {
        // Check if already committed
        self.check_not_committed()?;

        let _commit_guard = self.inner.mvcc().commit_lock.lock();

        // If serializable isolation is enabled, check for conflicts
        if let Some(key_hashes) = &self.key_hashes {
            let key_hashes = key_hashes.lock();
            let read_set = &key_hashes.1; // Get read set
            let write_set = &key_hashes.0; // Get write set

            // Get commit timestamp before validation
            let expected_commit_ts = self.inner.mvcc().latest_commit_ts() + 1;

            // Check for conflicts in committed transactions
            let committed_txns = self.inner.mvcc().committed_txns.lock();

            // both excluded bounds
            let lower_bound = Bound::Excluded(self.read_ts);
            let upper_bound = Bound::Excluded(expected_commit_ts);

            // Iterate through transactions committed after our read_ts
            for (&commit_ts, txn_data) in committed_txns.range((lower_bound, upper_bound)) {
                // Check if read set intersects with the committed transaction's write set
                for &hash in read_set {
                    if txn_data.key_hashes.contains(&hash) {
                        return Err(anyhow::anyhow!(
                            "[txn {}]serialization failure: read-write conflict detected with transaction committed at ts {}",
                            self.read_ts,
                            commit_ts
                        ));
                    }
                }
            }
            drop(committed_txns); // Release lock early

            // Collect all key-value pairs from local storage
            let mut write_batch = Vec::new();

            for entry in self.local_storage.iter() {
                let key = entry.key().clone();
                let value = entry.value().clone();
                write_batch.push(WriteBatchRecord::Put(key, value));
            }

            // Submit write batch to storage engine
            let commit_ts = self.inner.write_batch_lsm(&write_batch)?;

            // Record this transaction's write set
            let mut committed_txns = self.inner.mvcc().committed_txns.lock();
            committed_txns.insert(
                commit_ts,
                CommittedTxnData {
                    key_hashes: write_set.clone(),
                    read_ts: self.read_ts,
                    commit_ts,
                },
            );
        } else {
            // Non-serializable transaction - just write the batch
            let mut write_batch = Vec::new();

            for entry in self.local_storage.iter() {
                let key = entry.key().clone();
                let value = entry.value().clone();
                write_batch.push(WriteBatchRecord::Put(key, value));
            }
            self.inner.write_batch_lsm(&write_batch)?;
        }

        // Set committed flag to true
        self.committed
            .store(true, std::sync::atomic::Ordering::Release);

        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        // Get MVCC manager from storage
        if let Some(mvcc) = &self.inner.mvcc {
            let mut ts_lock = mvcc.ts.lock();
            // Remove this transaction's read timestamp from watermark
            ts_lock.1.remove_reader(self.read_ts);

            // Print watermark info after removal
            let debug_enabled = std::env::var("RUST_BACKTRACE")
                .map(|val| val == "1")
                .unwrap_or(false);
            if debug_enabled {
                println!(
                    "[Transaction Drop] Before removal - Current watermark: {:?}, Remaining readers: {:?}",
                    ts_lock.1.watermark(),
                    ts_lock.1.debug_readers()
                );
            }

            // When you commit a transaction, you can also clean up the committed txn map to remove all transactions below the watermark
            if self.committed.load(std::sync::atomic::Ordering::Acquire) {
                // Get current watermark
                let watermark = ts_lock.1.watermark().unwrap_or(ts_lock.0);
                // Clean up old transactions below watermark
                let mut committed_txns = self.inner.mvcc().committed_txns.lock();
                committed_txns.retain(|&ts, _| ts >= watermark);
            }
        }
    }
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
    pub fn create_empty(
        map: Arc<SkipMap<Bytes, Bytes>>,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Self {
        // Convert bounds to owned bytes for storage operations
        let lower_bytes = map_bound_to_bytes(_lower);
        let upper_bytes = map_bound_to_bytes(_upper);

        let mut iter = TxnLocalIteratorBuilder {
            map: map.clone(),
            iter_builder: |map| map.range((lower_bytes, upper_bytes)),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();

        // Move to first item
        iter.next().unwrap();
        iter
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

    fn skip_deletes(&mut self) -> Result<()> {
        while self.iter.is_valid() && self.iter.value().is_empty() {
            // Add key to read set before skipping
            if let Some(key_hashes) = &self._txn.key_hashes {
                let hash = farmhash::hash32(self.iter.key());
                let mut guard = key_hashes.lock();
                guard.1.insert(hash);
            }

            self.iter.next()?;
        }
        Ok(())
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
        // Add current key to read set before moving to next
        if self.is_valid() {
            if let Some(key_hashes) = &self._txn.key_hashes {
                let hash = farmhash::hash32(self.iter.key());
                let mut guard = key_hashes.lock();
                guard.1.insert(hash);
            }
        }

        // Simply delegate to the underlying iterator
        // The TwoMergeIterator will handle merging results from local storage and LSM storage
        self.iter.next()?;

        // given that the TwoMergeIterator will retain the deletion markers in the child iterators
        self.skip_deletes()?;

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
