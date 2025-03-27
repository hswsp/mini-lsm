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

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
/// When we scan from the storage engine, we will need to merge data from both memtable iterators and SST iterators into a single one
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    use_a: bool, // flag to indicate which iterator to read from
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> TwoMergeIterator<A, B>
{
    /// Helper function to determine which iterator to use
    fn select_iterator(&self) -> bool {
        if self.a.is_valid() && self.b.is_valid() {
            // Both valid - use A if its key is smaller or equal
            self.a.key() <= self.b.key()
        } else {
            // Use A if it's valid, otherwise use B
            self.a.is_valid()
        }
    }

    /// Skip any duplicate keys in B that match A's current key
    /// 所以这是有先后顺序的，默认A的优先级高于B,初始化的时候需要注意。
    fn skip_b_duplicates(&mut self) -> Result<()> {
        while self.a.is_valid() && self.b.is_valid() && self.b.key() == self.a.key() {
            self.b.next()?;
        }
        Ok(())
    }

    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = Self { a, b, use_a: true };

        // Initially choose the smaller key, or a if equal
        iter.use_a = iter.select_iterator();

        // Skip any duplicates at initialization
        iter.skip_b_duplicates()?;

        Ok(iter)
    }
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.use_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.use_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        (self.use_a && self.a.is_valid()) || (!self.use_a && self.b.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        if !self.is_valid() {
            return Ok(());
        }

        if self.use_a {
            // We were using iterator A
            self.a.next()?;
        } else {
            // We were using iterator B
            self.b.next()?;
        }

        // Skip duplicates using the helper method
        self.skip_b_duplicates()?;

        // Choose next iterator
        self.use_a = self.select_iterator();

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
