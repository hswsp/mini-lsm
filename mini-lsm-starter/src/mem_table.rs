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
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice, TS_DEFAULT, map_key_bound, map_key_bound_plus_ts};
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    pub map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// 范围查询示例
/// storage.scan(
///     Bound::Included(b"key1"),  // 从 "key1" 开始（包含）
///     Bound::Excluded(b"key5")   // 到 "key5" 结束（不包含）
/// )
impl MemTable {
    /// Create a new mem-table.
    pub fn create(id: usize) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(_id: usize, _path: impl AsRef<Path>) -> Result<Self> {
        let wal = Wal::create(_path)?;
        Ok(Self {
            map: Arc::new(SkipMap::new()),
            wal: Some(wal),
            id: _id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(_id: usize, _path: impl AsRef<Path>) -> Result<Self> {
        let map = Arc::new(SkipMap::new());
        // Recover WAL contents into the skiplist
        let wal = Wal::recover(_path, &map)?;

        // Calculate initial size
        let mut size = 0;
        for entry in map.iter() {
            size += entry.key().raw_len() + entry.value().len();
        }

        Ok(Self {
            map,
            wal: Some(wal),
            id: _id,
            approximate_size: Arc::new(AtomicUsize::new(size)),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let key_slice = KeySlice::from_slice(key, TS_DEFAULT);
        self.put(key_slice, value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(key)
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        self.scan(
            map_key_bound_plus_ts(lower, TS_DEFAULT),
            map_key_bound_plus_ts(upper, TS_DEFAULT),
        )
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let key_bytes = KeyBytes::from_bytes_with_ts(Bytes::copy_from_slice(key), TS_DEFAULT);
        self.map.get(&key_bytes).map(|e| e.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    /// In week 3, day 5, modify the function to use the batch API.
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        // Calculate total size for approximate size tracking
        let total_size: usize = _data
            .iter()
            .map(|(key, value)| key.raw_len() + value.len())
            .sum();

        // Update approximate size
        self.approximate_size
            .fetch_add(total_size, std::sync::atomic::Ordering::Relaxed);

        // Write to WAL first if it exists
        if let Some(ref wal) = self.wal {
            for (key, value) in _data {
                wal.put(*key, value)?;
            }
            // Note: If WAL batch write is needed, we can add it to WAL implementation
        }

        // Insert all key-value pairs into the skipmap
        for (key, value) in _data {
            let key_bytes =
                KeyBytes::from_bytes_with_ts(Bytes::copy_from_slice(key.key_ref()), key.ts());
            let value_bytes = Bytes::copy_from_slice(value);
            self.map.insert(key_bytes, value_bytes);
        }

        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<KeySlice>, upper: Bound<KeySlice>) -> MemTableIterator {
        let (lower_bound, upper_bound) = (map_key_bound(lower), map_key_bound(upper));

        let mut iterator = MemTableIteratorBuilder {
            map: self.map.clone(),
            item: (KeyBytes::new(), Bytes::new()),
            iter_builder: |map| map.range((lower_bound, upper_bound)),
        } // SkipMap::range 返回的迭代器本身就是按键升序排列的。
        .build();

        // 调用一次 next() 函数以获取第一个元素
        iterator.next().unwrap();

        // 处理 Excluded 边界情况
        if iterator.is_valid() {
            if let Bound::Excluded(excluded_key) = lower {
                // 如果当前键等于排除的键，需要跳过所有相同的键
                while iterator.is_valid() && iterator.key().key_ref() == excluded_key.key_ref() {
                    iterator.next().unwrap();
                }
            }
        }

        // 添加调试输出
        let debug_enabled = std::env::var("RUST_BACKTRACE")
            .map(|val| val == "1")
            .unwrap_or(false);

        if debug_enabled {
            if iterator.is_valid() {
                let key_str = String::from_utf8_lossy(iterator.key().key_ref());
                let value_str = String::from_utf8_lossy(iterator.value());
                println!(
                    "[MemTable Scan] Initial key-value pair: key='{}' (ts={}), value='{}'",
                    key_str,
                    iterator.key().ts(),
                    value_str
                );
            } else {
                println!("[MemTable Scan] Iterator is not valid after initial next()");
            }
        }

        iterator
    }

    /// Flush the mem-table to SSTable.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            let key = entry.key();
            let value = entry.value();
            builder.add(KeySlice::from_slice(key.key_ref(), key.ts()), value);
        }
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes,
>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
/// If the iterator does not have a lifetime generics parameter, we should ensure that whenever the iterator is being used,
/// the underlying skiplist object is not freed. The only way to achieve that is to put the Arc<SkipMap> object into the iterator itself.
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>, // 要么返回一个包含键值对的 Some(Entry<'a, K, V>)，要么在迭代结束时返回 None。
    /// Stores the current key-value pair.
    item: (KeyBytes, Bytes),
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        &self.borrow_item().1[..]
    }

    fn key(&self) -> KeySlice {
        let item = self.borrow_item();
        item.0.as_key_slice()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        // Store the new item outside the first closure
        let new_item = self.with_iter_mut(|iter| {
            iter.next() // 直接使用 SkipMap 的有序迭代器
                .map(|e| (e.key().clone(), e.value().clone()))
                .unwrap_or_default()
        });
        // Update the item in a separate borrow
        self.with_mut(|fields| {
            *fields.item = new_item;
        });
        Ok(())
    }
}
