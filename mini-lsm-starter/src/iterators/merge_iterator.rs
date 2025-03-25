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

use std::cmp::{self};
use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    /// 堆排序：先按 key 升序排列，key 相同时按索引升序
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key()) // 1. 先按 key 升序排列
            .then(self.0.cmp(&other.0)) // 2. key 相同时按索引升序
            .reverse() // 3. 反转顺序（因为 BinaryHeap 是最大堆）
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        // 创建一个优先队列来存储有效的迭代器，这里前提是每一个迭代器有序
        let mut heap = BinaryHeap::new();

        // 将所有有效的迭代器加入堆中
        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(idx, iter));
            }
        }

        // 从堆中取出第一个迭代器作为当前迭代器
        let current = heap.pop();

        Self {
            iters: heap,
            current,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        if !self.is_valid() {
            return Ok(());
        }

        // 当我们移动迭代器时（调用 next()），原来的引用可能会失效.
        // to_key_vec() 获取数据的所有权，确保数据在整个 next() 方法执行期间都有效
        let current_key = self.key().to_key_vec();

        // Advance current iterator
        // 保证current始终存储从iters.pop()出来的迭代器
        if let Some(mut current) = self.current.take() {
            current.1.next()?;
            // If the current iterator is no longer valid, we need to pop it from heap.
            if current.1.is_valid() {
                self.iters.push(current);
            }
        }

        // Process all entries with the same key using peek_mut
        while let Some(mut inner_iter) = self.iters.peek_mut() {
            debug_assert!(
                inner_iter.1.key() >= KeySlice::from_slice(current_key.raw_ref()),
                "heap invariant violated"
            );

            if inner_iter.1.key() == KeySlice::from_slice(current_key.raw_ref()) {
                // Case 1: an error occurred when calling `next`.
                if let e @ Err(_) = inner_iter.1.next() {
                    // we still need to pop the iterator from heap
                    PeekMut::pop(inner_iter);
                    return e;
                }
                // Case 2: iter is no longer valid.
                if !inner_iter.1.is_valid() {
                    PeekMut::pop(inner_iter);
                }
            } else {
                break;
            }
        }

        // Get next smallest element
        self.current = self.iters.pop();

        Ok(())
    }
}
