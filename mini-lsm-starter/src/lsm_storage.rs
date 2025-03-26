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

use std::collections::HashMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::StorageIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::KeySlice;
use crate::mem_table::map_bound;
use crate::table::{SsTableBuilder, SsTableIterator};
use crate::{
    iterators::merge_iterator::MergeIterator,
    lsm_iterator::{FusedIterator, LsmIterator},
    manifest::Manifest,
    mem_table::{MemTable, MemTableIterator},
    mvcc::LsmMvccInner,
    table::SsTable,
};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        // Signal the threads to stop
        self.flush_notifier.send(())?;
        self.compaction_notifier.send(())?;

        // Take the thread handles
        let mut flush_thread = self.flush_thread.lock();
        let mut compaction_thread = self.compaction_thread.lock();

        // Wait for threads to finish
        if let Some(thread) = flush_thread.take() {
            thread
                .join()
                .map_err(|e| anyhow::anyhow!("flush thread panicked: {:?}", e))?;
        }

        if let Some(thread) = compaction_thread.take() {
            thread
                .join()
                .map_err(|e| anyhow::anyhow!("compaction thread panicked: {:?}", e))?;
        }

        // Flush any remaining data in memtables
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        while !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }

        self.inner.sync_dir()?;

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

fn range_overlap(
    user_begin: Bound<&[u8]>,
    user_end: Bound<&[u8]>,
    table_begin: KeySlice,
    table_end: KeySlice,
) -> bool {
    match user_end {
        Bound::Excluded(key) if key <= table_begin.raw_ref() => {
            return false;
        }
        Bound::Included(key) if key < table_begin.raw_ref() => {
            return false;
        }
        _ => {}
    }
    match user_begin {
        Bound::Excluded(key) if key >= table_end.raw_ref() => {
            return false;
        }
        Bound::Included(key) if key > table_end.raw_ref() => {
            return false;
        }
        _ => {}
    }
    true
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        // Create the directory if it doesn't exist
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }

        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    /// delete implementation should simply put an empty slice for that key
    pub fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        // 1. 首先检查 memtable
        if let Some(value) = snapshot.memtable.get(_key) {
            // 检查是否是删除标记
            if value.is_empty() {
                return Ok(None);
            }
            return Ok(Some(value));
        }

        // 2. 检查 immutable memtables
        for memtable in snapshot.imm_memtables.iter() {
            if let Some(value) = memtable.get(_key) {
                // 检查是否是删除标记
                if value.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        // 3. 检查 SSTable 层
        // TODO: 在实现 SSTable 后添加对 SSTable 的搜索
        let merge_iter = self.create_sst_merge_iterator(
            &snapshot,
            Bound::Included(_key),
            Bound::Included(_key),
        )?;
        if !merge_iter.is_valid() {
            return Ok(None);
        }

        // Check if we found the exact key
        if merge_iter.key().raw_ref() == _key {
            let value = merge_iter.value();
            if value.is_empty() {
                Ok(None)
            } else {
                Ok(Some(Bytes::copy_from_slice(value)))
            }
        } else {
            Ok(None)
        }
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], value: &[u8]) -> Result<()> {
        let need_freeze = {
            // 获取读锁的作用域
            let state = self.state.read();
            // 写入 memtable
            state.memtable.put(_key, value)?;
            // 检查是否需要冻结，记录结果
            state.memtable.approximate_size() >= self.options.target_sst_size
        }; // 这里读锁就被释放了

        // 如果俩线程同时reaches capacity limit.
        // They will both do a size check on the memtable and decide to freeze it.
        // In this case, we might create one empty memtable which is then immediately frozen.
        // 如果需要冻结，在读锁释放后再进行冻结操作
        if need_freeze {
            // 获取状态锁来进行 memtable 冻结
            let state_lock = self.state_lock.lock();
            // 再次检查大小，因为可能其他线程已经处理了
            if self.state.read().memtable.approximate_size() >= self.options.target_sst_size {
                self.force_freeze_memtable(&state_lock)?;
            }
        }

        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        // 写入空值作为删除标记
        self.put(_key, &[])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        let dir = std::fs::File::open(&self.path)?;
        dir.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        // 在加锁前创建新的 memtable
        let new_memtable = Arc::new(MemTable::create(self.next_sst_id()));

        // 获取写锁来修改状态
        let mut state = self.state.write();

        // 检查 immutable memtables 数量是否达到限制
        if state.imm_memtables.len() >= self.options.num_memtable_limit {
            // 如果达到Maximum number of memtables in memory，先刷新一个 immutable memtable 到磁盘
            drop(state); // 释放写锁，避免死锁
            self.force_flush_next_imm_memtable()?;
            state = self.state.write(); // 重新获取写锁
        }

        // 替换当前 memtable，将旧的移动到 immutable memtables
        let new_state = Arc::new(LsmStorageState {
            memtable: new_memtable,
            imm_memtables: {
                let mut new_imm = Vec::with_capacity(state.imm_memtables.len() + 1);
                // 将当前 memtable 放在最前面（最新的）
                new_imm.push(state.memtable.clone());
                // 添加现有的 immutable memtables
                new_imm.extend(state.imm_memtables.iter().cloned());
                new_imm
            },
            l0_sstables: state.l0_sstables.clone(),
            levels: state.levels.clone(),
            sstables: state.sstables.clone(),
        });

        // 原子地更新状态
        *state = new_state;

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        // Take state lock to ensure only one flush operation at a time
        let _state_lock = self.state_lock.lock();

        // Get the memtable to flush while holding a read lock
        let memtable_to_flush = {
            let state = self.state.read();
            if state.imm_memtables.is_empty() {
                return Ok(());
            }
            state.imm_memtables.last().cloned().unwrap()
        };

        // Create and build SST outside of any locks
        let sst_id = self.next_sst_id();
        let mut builder = SsTableBuilder::new(self.options.block_size);
        memtable_to_flush.flush(&mut builder)?;

        let sst = builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?;
        let sst = Arc::new(sst);
        // Update state with write lock
        {
            let mut state = self.state.write();
            // Double check the memtable is still there and is the same one
            if let Some(last) = state.imm_memtables.last() {
                if Arc::ptr_eq(last, &memtable_to_flush) {
                    // Create new state with modifications
                    let new_state = Arc::new(LsmStorageState {
                        memtable: state.memtable.clone(),
                        imm_memtables: {
                            let mut new_imm = state.imm_memtables.clone();
                            new_imm.pop(); // Remove the last memtable
                            new_imm
                        },
                        l0_sstables: {
                            let mut new_l0 = Vec::with_capacity(state.l0_sstables.len() + 1);
                            new_l0.push(sst_id); // Add newest SST first
                            new_l0.extend(state.l0_sstables.iter().cloned()); // Add existing SSTs
                            new_l0
                        },
                        levels: state.levels.clone(),
                        sstables: {
                            let mut new_sst = state.sstables.clone();
                            new_sst.insert(sst_id, sst);
                            new_sst
                        },
                    });

                    // Replace the old state with the new one
                    *state = new_state;
                }
            }
        }

        // Sync directory after releasing the write lock
        self.sync_dir()?;

        println!(
            "Flushed memtable {} to L0 SST {}",
            memtable_to_flush.id(),
            sst_id
        );

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        // Take a snapshot of the state to release the lock early
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        // 收集所有 memtable 迭代器
        let mut mem_iters = Vec::new();

        // 添加当前 memtable
        mem_iters.push(snapshot.memtable.scan(_lower, _upper));

        // 添加 immutable memtables
        // 最新的在最前面
        for imm_mem in &snapshot.imm_memtables {
            mem_iters.push(imm_mem.scan(_lower, _upper));
        }

        // 转换为 Box<MemTableIterator> 的集合
        let boxed_iters: Vec<Box<MemTableIterator>> = mem_iters.into_iter().map(Box::new).collect();
        // 创建合并迭代器
        let mem_merge_iter = MergeIterator::create(boxed_iters);

        // Create SSTable iterators (outside the critical section)

        let sst_merge_iter = self.create_sst_merge_iterator(&snapshot, _lower, _upper)?;
        let two_merge_iter = TwoMergeIterator::create(mem_merge_iter, sst_merge_iter)?;

        // Create LSM iterator with upper bound
        let lsm_iter = LsmIterator::new(two_merge_iter, map_bound(_upper))?;

        Ok(FusedIterator::new(lsm_iter))
    }

    fn create_sst_iterator(sst: &Arc<SsTable>, lower: Bound<&[u8]>) -> Result<SsTableIterator> {
        match lower {
            Bound::Included(key) => {
                SsTableIterator::create_and_seek_to_key(sst.clone(), KeySlice::from_slice(key))
            }
            Bound::Excluded(key) => {
                // Create iterator and seek to key
                let key = KeySlice::from_slice(key);
                let mut iter = SsTableIterator::create_and_seek_to_key(sst.clone(), key)?;
                // If the iterator points to the excluded key, move to next key
                if iter.is_valid() && iter.key() == key {
                    iter.next()?;
                }
                Ok(iter)
            }
            Bound::Unbounded => SsTableIterator::create_and_seek_to_first(sst.clone()),
        }
    }

    fn create_sst_merge_iterator(
        &self,
        snapshot: &Arc<LsmStorageState>,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<MergeIterator<SsTableIterator>> {
        let mut sst_iters = Vec::new();

        // Add L0 SSTs (newest to oldest)
        // 注意存入的时候需要按newest to oldest的顺序存入
        for &sst_id in snapshot.l0_sstables.iter() {
            if let Some(sst) = snapshot.sstables.get(&sst_id) {
                // filter out some SSTs that do not contain the key range,
                // so that we do not need to read them in the merge iterator
                if range_overlap(
                    lower,
                    upper,
                    sst.first_key().as_key_slice(),
                    sst.last_key().as_key_slice(),
                ) {
                    sst_iters.push(Self::create_sst_iterator(sst, lower)?);
                }
            }
        }

        // Add other levels
        for (_, level_ssts) in &snapshot.levels {
            //注意存入的时候需要按newest to oldest的顺序存入
            for &sst_id in level_ssts.iter() {
                if let Some(sst) = snapshot.sstables.get(&sst_id) {
                    if range_overlap(
                        lower,
                        upper,
                        sst.first_key().as_key_slice(),
                        sst.last_key().as_key_slice(),
                    ) {
                        sst_iters.push(Self::create_sst_iterator(sst, lower)?);
                    }
                }
            }
        }

        Ok(MergeIterator::create(
            sst_iters.into_iter().map(Box::new).collect(),
        ))
    }
}
