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
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
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
        // 创建一个优先队列来存储有效的迭代器
        let mut heap = BinaryHeap::new();
        
        // 将所有有效的迭代器加入堆中
        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(idx, iter));
            }
        }

        // 从堆中取出第一个迭代器作为当前迭代器
        let current = heap.pop();
        
        Self { iters: heap, current }
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
        // 如果当前没有有效的迭代器，直接返回
        if !self.is_valid() {
            return Ok(());
        }

        // 提前获取当前 key 并转换为 owned 类型
        let current_key = self.key().to_key_vec();

        // 移动当前迭代器到下一个位置
        if let Some(mut current) = self.current.take() {
            current.1.next()?;
            // 如果当前迭代器仍然有效，将其放回堆中
            if current.1.is_valid() {
                self.iters.push(current);
            }
        }

        // 处理堆中所有相同 key 的迭代器（使用临时堆避免借用冲突）
        let mut temp_heap = std::mem::take(&mut self.iters);
        let mut remaining = BinaryHeap::new();
        
        while let Some(mut iter) = temp_heap.pop() {
            if iter.1.key() == KeySlice::from_slice(current_key.raw_ref()) {
                iter.1.next()?;
                if iter.1.is_valid() {
                    remaining.push(iter);
                }
            } else {
                remaining.push(iter);
            }
        }
        self.iters = remaining;

        // 从堆中取出下一个最小元素
        self.current = self.iters.pop();
        Ok(())
    }
}
