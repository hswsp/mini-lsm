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

use std::collections::{BTreeSet, HashMap};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::{Context, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::{KeySlice, TS_RANGE_BEGIN};
use crate::manifest::ManifestRecord;
use crate::mem_table::map_bound;
use crate::table::{FileObject, SsTableBuilder, SsTableIterator};
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
        self.inner.sync_dir()?; // Sync the directory before closing
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

        // Only flush memtables if WAL is disabled
        if !self.inner.options.enable_wal {
            // Flush any remaining data in memtables
            // create memtable and skip updating manifest
            if !self.inner.state.read().memtable.is_empty() {
                // 因为最后生成的memtable已经不再使用，无需新建memtable并update相应的manifest
                self.inner
                    .update_imm_memtable_for_freeze(Arc::new(MemTable::create(
                        self.inner.next_sst_id(),
                    )))?;
            }
            while !self.inner.state.read().imm_memtables.is_empty() {
                self.inner.force_flush_next_imm_memtable()?;
            }
        } else {
            // If WAL is enabled, just sync all WALs
            // 当期那的活跃的memtable也当做imm memtable，并flush
            // recover的时候直接恢复到imm_memtable里
            self.sync()?;

            println!("-------------  print LSM structure before close -----------------");
            self.dump_structure();
            self.inner.dump_memtables();
        }

        self.inner.sync_dir()?;
        println!("-------------  storage closed -----------------");
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
        Bound::Excluded(key) if key <= table_begin.key_ref() => {
            return false;
        }
        Bound::Included(key) if key < table_begin.key_ref() => {
            return false;
        }
        _ => {}
    }
    match user_begin {
        Bound::Excluded(key) if key >= table_end.key_ref() => {
            return false;
        }
        Bound::Included(key) if key > table_end.key_ref() => {
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

    /// Create a new memtable (potentially with WAL) as a static method
    fn create_memtable_static(
        path: &Path,
        options: &LsmStorageOptions,
        id: usize,
    ) -> Result<Arc<MemTable>> {
        if options.enable_wal {
            // Create memtable with WAL
            let wal_path = Self::path_of_wal_static(path, id);
            Ok(Arc::new(MemTable::create_with_wal(id, wal_path)?))
        } else {
            // Create simple memtable without WAL
            Ok(Arc::new(MemTable::create(id)))
        }
    }

    fn create_memtable(&self, id: usize) -> Result<Arc<MemTable>> {
        Self::create_memtable_static(&self.path, &self.options, id)
    }

    /// Start the storage engine by either loading an existing directory
    /// or creating a new one if the directory does not exist.
    fn create_new_storage(
        path: &Path,
        options: &LsmStorageOptions,
        block_cache: Arc<BlockCache>,
        compaction_controller: CompactionController,
    ) -> Result<Self> {
        let mut state = LsmStorageState::create(options);
        let mut next_sst_id = 1;

        // Create new manifest
        let manifest_path = path.join("MANIFEST");
        let manifest = Manifest::create(&manifest_path).context("failed to create manifest")?;

        // Create initial memtable
        state.memtable = Self::create_memtable_static(path, options, next_sst_id)?;
        manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
        next_sst_id += 1;

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: options.clone().into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };
        storage.sync_dir()?;
        Ok(storage)
    }

    fn recover_from_storage(
        path: &Path,
        options: &LsmStorageOptions,
        block_cache: Arc<BlockCache>,
        compaction_controller: CompactionController,
    ) -> Result<Self> {
        let mut state = LsmStorageState::create(options);
        let manifest_path = path.join("MANIFEST");

        // Recover from manifest
        let (manifest, records) = Manifest::recover(&manifest_path)?;
        let mut memtables = BTreeSet::new(); // 注意这里不能使用HashSet， 我们必须保证imm_memtables的有序性！
        let mut next_sst_id = 1;

        // first generate the list of SSTs you will need to load
        for record in records {
            match record {
                ManifestRecord::Flush(sst_id) => {
                    let res = memtables.remove(&sst_id);
                    assert!(res, "memtable {:05} not exist?", sst_id);
                    if compaction_controller.flush_to_l0() {
                        state.l0_sstables.insert(0, sst_id);
                    } else {
                        state.levels.insert(0, (sst_id, vec![sst_id]));
                    }
                    // compute the maximum SST id and update the next_sst_id field.
                    next_sst_id = next_sst_id.max(sst_id);
                }
                ManifestRecord::NewMemtable(x) => {
                    next_sst_id = next_sst_id.max(x);
                    memtables.insert(x);
                }
                ManifestRecord::Compaction(task, output) => {
                    let (new_state, _) =
                        compaction_controller.apply_compaction_result(&state, &task, &output, true);
                    // TODO: apply remove again
                    state = new_state;
                    next_sst_id = next_sst_id.max(output.iter().max().copied().unwrap_or_default());
                }
            }
        }

        let mut sst_cnt = 0;
        // recover SSTs
        for table_id in state
            .l0_sstables
            .iter()
            .chain(state.levels.iter().flat_map(|(_, files)| files))
        {
            let table_id = *table_id;
            let sst = SsTable::open(
                table_id,
                Some(block_cache.clone()),
                FileObject::open(&Self::path_of_sst_static(path, table_id))
                    .with_context(|| format!("failed to open SST: {}", table_id))?,
            )?;
            state.sstables.insert(table_id, Arc::new(sst));
            sst_cnt += 1;
        }
        println!("{} SSTs opened", sst_cnt);

        next_sst_id += 1;

        // Sort SSTs on each level (only for leveled compaction)
        if let CompactionController::Leveled(_) = &compaction_controller {
            for (_id, ssts) in &mut state.levels {
                ssts.sort_by_cached_key(|sst_id| state.sstables[sst_id].first_key().clone());
            }
        }

        // recover memtables
        if options.enable_wal {
            let mut wal_cnt = 0;
            // Try to recover existing memtables from WAL
            for &memtable_id in &memtables {
                let wal_path = Self::path_of_wal_static(path, memtable_id);
                assert!(wal_path.exists(), "WAL file not found: {:?}", wal_path);
                let recovered_memtable = MemTable::recover_from_wal(memtable_id, wal_path)?;
                if !recovered_memtable.is_empty() {
                    // 可能上一次close sync的时候，memtable是空的，没必要recover了
                    // 注意id越大的，越插到前面
                    state.imm_memtables.insert(0, Arc::new(recovered_memtable));
                    wal_cnt += 1;
                }
            }
            println!("{} WALs recovered", wal_cnt);
        }

        // Create new memtable
        state.memtable = Self::create_memtable_static(path, options, next_sst_id)?;
        manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
        next_sst_id += 1;

        let storage: LsmStorageInner = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: options.clone().into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };
        storage.sync_dir()?;

        println!("-------------  print LSM structure after opened again -----------------");
        storage.dump_memtables();
        storage.dump_structure();
        println!("-------------  storage opened -----------------");

        Ok(storage)
    }

    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let block_cache = Arc::new(BlockCache::new(1 << 20)); // 4GB block cache,

        let path = path.as_ref();
        // Create the directory if it doesn't exist
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }

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

        let manifest_path = path.join("MANIFEST");
        if !manifest_path.exists() {
            Self::create_new_storage(path, &options, block_cache, compaction_controller)
        } else {
            Self::recover_from_storage(path, &options, block_cache, compaction_controller)
        }
    }

    pub fn sync(&self) -> Result<()> {
        // Get current memtable
        let state = self.state.read();

        // Sync current memtable's WAL
        state.memtable.sync_wal()?;

        // immutable memtables' WALs have alrealy been synced in freeze_memtable
        Ok(())
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

        // Create key slice with largest timestamp for initial lookup
        let key_with_ts = KeySlice::from_slice(_key, TS_RANGE_BEGIN);

        // 1. Check memtable
        if let Some(value) = snapshot.memtable.get(_key) {
            // 检查是否是删除标记
            if value.is_empty() {
                return Ok(None);
            }
            return Ok(Some(value));
        }

        // 2. Check immutable memtables
        for memtable in snapshot.imm_memtables.iter() {
            if let Some(value) = memtable.get(_key) {
                // 检查是否是删除标记
                if value.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        // 3. Check L0 SSTs
        for &sst_id in snapshot.l0_sstables.iter() {
            if let Some(sst) = snapshot.sstables.get(&sst_id) {
                // Skip if key is not in SST range
                if _key < sst.first_key().key_ref() || _key > sst.last_key().key_ref() {
                    continue;
                }

                // Check bloom filter before reading
                if !sst.may_contain(_key) {
                    continue;
                }

                let iter = SsTableIterator::create_and_seek_to_key(sst.clone(), key_with_ts)?;
                if iter.is_valid() && iter.key().key_ref() == _key {
                    return if iter.value().is_empty() {
                        Ok(None)
                    } else {
                        Ok(Some(Bytes::copy_from_slice(iter.value())))
                    };
                }
            }
        }
        // 4. Check other levels
        for (_, level_ssts) in &snapshot.levels {
            for &sst_id in level_ssts {
                if let Some(sst) = snapshot.sstables.get(&sst_id) {
                    // Skip if key is not in SST range
                    if _key < sst.first_key().key_ref() || _key > sst.last_key().key_ref() {
                        continue;
                    }
                    // Check bloom filter before reading
                    if !sst.may_contain(_key) {
                        continue;
                    }
                    let iter = SsTableIterator::create_and_seek_to_key(sst.clone(), key_with_ts)?;
                    if iter.is_valid() && iter.key().key_ref() == _key {
                        return if iter.value().is_empty() {
                            Ok(None)
                        } else {
                            Ok(Some(Bytes::copy_from_slice(iter.value())))
                        };
                    }
                }
            }
        }

        Ok(None)
    }

    fn try_freeze(&self, estimated_size: usize) -> Result<()> {
        // 如果需要冻结，在读锁释放后再进行冻结操作
        if estimated_size < self.options.target_sst_size {
            return Ok(());
        }
        // 检查 immutable memtables 数量是否达到限制
        if self.state.read().imm_memtables.len() + 1 >= self.options.num_memtable_limit {
            // Flush one immutable memtable to make room
            self.force_flush_next_imm_memtable()?;
        }
        // 获取状态锁来进行 memtable 冻结
        let state_lock = self.state_lock.lock();
        // 再次检查大小，因为可能其他线程已经处理了
        if self.state.read().memtable.approximate_size() >= self.options.target_sst_size {
            self.force_freeze_memtable(&state_lock)?;
        }
        Ok(())
    }
    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        let estimated_size = {
            // Get read lock scope for initial write to memtable
            let state = self.state.read();

            // Process each record in the batch
            for record in _batch {
                match record {
                    WriteBatchRecord::Put(key, value) => {
                        state.memtable.put(key.as_ref(), value.as_ref())?;
                    }
                    WriteBatchRecord::Del(key) => {
                        // Delete by writing empty value
                        state.memtable.put(key.as_ref(), &[])?;
                    }
                }
            }

            // Get approximate size after batch write
            state.memtable.approximate_size()
        }; // Read lock is released here

        // Check if memtable needs to be frozen after batch write
        // 如果俩线程同时reaches capacity limit.
        // They will both do a size check on the memtable and decide to freeze it.
        // In this case, we might create one empty memtable which is then immediately frozen.
        self.try_freeze(estimated_size)?;

        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(_key, value)])
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(_key)])
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

    /// Updates the storage state by replacing the current memtable with a new one
    /// and moving the old memtable to immutable memtables.
    pub fn update_imm_memtable_for_freeze(&self, new_memtable: Arc<MemTable>) -> Result<()> {
        // 获取写锁来修改状态
        let mut state = self.state.write();
        // Swap the current memtable with a new one.
        let mut snapshot = state.as_ref().clone();
        let old_memtable = std::mem::replace(&mut snapshot.memtable, new_memtable);

        // Add the memtable to the immutable memtables
        snapshot.imm_memtables.insert(0, old_memtable.clone());

        // Atomically update the state
        *state = Arc::new(snapshot);
        drop(state);

        // Sync the old memtable's WAL file
        old_memtable.sync_wal()?;

        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        // 在加锁前创建新的 memtable
        let memtable_id = self.next_sst_id();
        let new_memtable = self.create_memtable(memtable_id)?;

        // Update state with the new memtable
        self.update_imm_memtable_for_freeze(new_memtable)?;

        self.manifest.as_ref().unwrap().add_record(
            _state_lock_observer,
            ManifestRecord::NewMemtable(memtable_id),
        )?;
        self.sync_dir()?;

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        // Get the memtable to flush while holding a read lock
        let memtable_to_flush = {
            let state = self.state.read();
            if state.imm_memtables.is_empty() {
                return Ok(());
            }
            state.imm_memtables.last().cloned().unwrap()
        };

        // Create and build SST outside of any locks.
        // 注意这里sst_id就是当前memtable的id, 保证manifest记录Flush与NewMemtable一一对应！
        let sst_id = memtable_to_flush.id();
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
            // Take state lock to ensure only one flush operation at a time
            let _state_lock = self.state_lock.lock();
            let state = self.state.read().as_ref().clone();
            // Double check the memtable is still there and is the same one
            if let Some(last) = state.imm_memtables.last() {
                if !Arc::ptr_eq(last, &memtable_to_flush) {
                    println!(
                        "skip flushing {}.sst with size={}, other thread has done this",
                        sst_id,
                        sst.table_size()
                    );
                    return Ok(());
                }
                // Create new state with modifications
                let mut new_state = LsmStorageState {
                    memtable: state.memtable.clone(),
                    imm_memtables: {
                        let mut new_imm = state.imm_memtables.clone();
                        new_imm.pop(); // Remove the last memtable
                        new_imm
                    },
                    l0_sstables: state.l0_sstables.clone(),
                    levels: state.levels.clone(),
                    sstables: {
                        let mut new_sst = state.sstables.clone();
                        new_sst.insert(sst_id, sst.clone());
                        new_sst
                    },
                };

                // Add SST based on compaction strategy
                if self.compaction_controller.flush_to_l0() {
                    // Add to L0 if using leveled compaction
                    new_state.l0_sstables.insert(0, sst_id);
                } else {
                    // Add as new tier if using tiered compaction
                    new_state.levels.insert(0, (sst_id, vec![sst_id]));
                }
                // Replace the old state with the new one
                println!("flushed {}.sst with size={}", sst_id, sst.table_size());

                *self.state.write() = Arc::new(new_state);
            }

            if self.options.enable_wal {
                // 不要漏了flush完之后删除WAL
                std::fs::remove_file(self.path_of_wal(sst_id))?;
            }

            // SST flush record stores the SST id that gets flushed to the disk
            self.manifest
                .as_ref()
                .unwrap()
                .add_record(&_state_lock, ManifestRecord::Flush(sst_id))?;
        }

        // Sync directory after releasing the write lock
        self.sync_dir()?;

        // println!(
        //     "Flushed memtable {} to L0 SST {}",
        //     memtable_to_flush.id(),
        //     sst_id
        // );

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
        // 创建MemTableIterator
        let mem_merge_iter = MergeIterator::create(boxed_iters);
        {
            // Create separate iterators for debug printing
            println!("\n=== Memtable Keys (Active + Immutable) ===");
            let mut debug_iters = Vec::new();
            debug_iters.push(snapshot.memtable.scan(_lower, _upper));
            for imm_mem in &snapshot.imm_memtables {
                debug_iters.push(imm_mem.scan(_lower, _upper));
            }
            let mut debug_merge_iter =
                MergeIterator::create(debug_iters.into_iter().map(Box::new).collect());

            // Print merged keys
            while debug_merge_iter.is_valid() {
                let key_str = String::from_utf8_lossy(debug_merge_iter.key().into_inner());
                let value_str = String::from_utf8_lossy(debug_merge_iter.value());
                println!("Key: '{}', Value: '{}'", key_str, value_str);
                debug_merge_iter.next()?;
            }
        }

        // Create SSTable iterators (outside the critical section)
        let l0_merge_iter = self.create_l0_sst_iter(&snapshot, _lower, _upper)?;
        // Debug print L0 SST keys
        {
            println!("\n=== L0 SST Keys ===");
            let mut debug_l0_iter = self.create_l0_sst_iter(&snapshot, _lower, _upper)?;
            while debug_l0_iter.is_valid() {
                let key_str = String::from_utf8_lossy(debug_l0_iter.key().key_ref());
                let value_str = String::from_utf8_lossy(debug_l0_iter.value());
                println!("Key: '{}', Value: '{}'", key_str, value_str);
                debug_l0_iter.next()?;
            }
        }
        let level_merge_iter = self.create_level_ssts_merge_iterator(&snapshot, _lower, _upper)?;

        let l0_sst_merge_iter = TwoMergeIterator::create(mem_merge_iter, l0_merge_iter)?;
        // Debug print merged keys from memtable and L0
        {
            println!("\n=== Merged Memtable + L0 Keys ===");
            let mut debug_mem_iters = Vec::new();
            debug_mem_iters.push(snapshot.memtable.scan(_lower, _upper));
            for imm_mem in &snapshot.imm_memtables {
                debug_mem_iters.push(imm_mem.scan(_lower, _upper));
            }
            let debug_mem_merge_iter =
                MergeIterator::create(debug_mem_iters.into_iter().map(Box::new).collect());
            let debug_l0_iter = self.create_l0_sst_iter(&snapshot, _lower, _upper)?;
            let mut debug_merged = TwoMergeIterator::create(debug_mem_merge_iter, debug_l0_iter)?;

            while debug_merged.is_valid() {
                let key_str = String::from_utf8_lossy(debug_merged.key().key_ref());
                let value_str = String::from_utf8_lossy(debug_merged.value());
                println!("Key: '{}', Value: '{}'", key_str, value_str);
                debug_merged.next()?;
            }
        }
        let two_merge_iter = TwoMergeIterator::create(l0_sst_merge_iter, level_merge_iter)?;

        // Create LSM iterator with upper bound
        let lsm_iter = LsmIterator::new(two_merge_iter, map_bound(_upper))?;

        Ok(FusedIterator::new(lsm_iter))
    }

    /// for l0 sst iterator
    fn create_sst_iterator(sst: &Arc<SsTable>, lower: Bound<&[u8]>) -> Result<SsTableIterator> {
        match lower {
            Bound::Included(key) => SsTableIterator::create_and_seek_to_key(
                sst.clone(),
                KeySlice::from_slice(key, TS_RANGE_BEGIN),
            ),
            Bound::Excluded(key) => {
                // Create iterator and seek to key
                let key = KeySlice::from_slice(key, TS_RANGE_BEGIN);
                let mut iter = SsTableIterator::create_and_seek_to_key(sst.clone(), key)?;
                // If the iterator points to the excluded key, move to next key
                println!(
                    "Excluded key comparison - Iterator key: '{}' (ts: {}), Search key: '{}' (ts: {})",
                    String::from_utf8_lossy(iter.key().key_ref()),
                    iter.key().ts(),
                    String::from_utf8_lossy(key.key_ref()),
                    key.ts()
                );
                if iter.is_valid() && iter.key().key_ref() == key.key_ref() {
                    iter.next()?;
                }
                Ok(iter)
            }
            Bound::Unbounded => SsTableIterator::create_and_seek_to_first(sst.clone()),
        }
    }

    /// for level sst iterator
    fn create_sst_concat_iterator(
        ssts: Vec<Arc<SsTable>>,
        lower: Bound<&[u8]>,
    ) -> Result<SstConcatIterator> {
        match lower {
            Bound::Included(key) => SstConcatIterator::create_and_seek_to_key(
                ssts,
                KeySlice::from_slice(key, TS_RANGE_BEGIN),
            ),
            Bound::Excluded(key) => {
                let key = KeySlice::from_slice(key, TS_RANGE_BEGIN);
                let mut iter = SstConcatIterator::create_and_seek_to_key(ssts, key)?;
                if iter.is_valid() && iter.key().key_ref() == key.key_ref() {
                    iter.next()?;
                }
                Ok(iter)
            }
            Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(ssts),
        }
    }

    fn create_l0_sst_iter(
        &self,
        snapshot: &Arc<LsmStorageState>,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<MergeIterator<SsTableIterator>> {
        let mut l0_iters = Vec::new();

        // Add L0 SSTs (newest to oldest)
        // 注意存入的时候需要按newest to oldest的顺序存入
        for &sst_id in snapshot.l0_sstables.iter() {
            if let Some(sst) = snapshot.sstables.get(&sst_id) {
                if range_overlap(
                    lower,
                    upper,
                    sst.first_key().as_key_slice(),
                    sst.last_key().as_key_slice(),
                ) {
                    l0_iters.push(Self::create_sst_iterator(sst, lower)?);
                }
            }
        }
        Ok(MergeIterator::create(
            l0_iters.into_iter().map(Box::new).collect(),
        ))
    }

    fn create_level_ssts_merge_iterator(
        &self,
        snapshot: &Arc<LsmStorageState>,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<MergeIterator<SstConcatIterator>> {
        let mut sst_iters = Vec::new();
        // Create L1+ iterator
        for (_, level_ssts) in &snapshot.levels {
            let mut ssts = Vec::new();
            //注意存入的时候需要按newest to oldest的顺序存入
            for &sst_id in level_ssts.iter() {
                if let Some(sst) = snapshot.sstables.get(&sst_id) {
                    if range_overlap(
                        lower,
                        upper,
                        sst.first_key().as_key_slice(),
                        sst.last_key().as_key_slice(),
                    ) {
                        ssts.push(Arc::clone(sst));
                    }
                }
            }
            sst_iters.push(Self::create_sst_concat_iterator(ssts, lower)?);
        }

        Ok(MergeIterator::create(
            sst_iters.into_iter().map(Box::new).collect(),
        ))
    }
}
