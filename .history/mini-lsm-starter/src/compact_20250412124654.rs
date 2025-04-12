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

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

/// Wrapper for SsTableIterator to implement StorageIterator
/// Rust 无法将具有泛型关联类型的 trait 用作 trait object
/// 这里使用枚举将所有可能的类型包装起来
enum SsTableIteratorWrapper<I>
where
    I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
{
    All(MergeIterator<SstConcatIterator>),
    Merge(TwoMergeIterator<MergeIterator<I>, SstConcatIterator>),
    Two(TwoMergeIterator<SstConcatIterator, SstConcatIterator>),
}

// Implement StorageIterator for the wrapper
impl<I> StorageIterator for SsTableIteratorWrapper<I>
where
    I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
{
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        match self {
            Self::All(iter) => iter.value(),
            Self::Two(iter) => iter.value(),
            Self::Merge(iter) => iter.value(),
        }
    }

    fn key(&self) -> Self::KeyType<'_> {
        match self {
            Self::All(iter) => iter.key(),
            Self::Two(iter) => iter.key(),
            Self::Merge(iter) => iter.key(),
        }
    }

    fn is_valid(&self) -> bool {
        match self {
            Self::All(iter) => iter.is_valid(),
            Self::Two(iter) => iter.is_valid(),
            Self::Merge(iter) => iter.is_valid(),
        }
    }

    fn next(&mut self) -> Result<()> {
        match self {
            Self::All(iter) => iter.next(),
            Self::Two(iter) => iter.next(),
            Self::Merge(iter) => iter.next(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn seal_sst(&self, builder: SsTableBuilder, new_ssts: &mut Vec<Arc<SsTable>>) -> Result<()> {
        let sst_id = self.next_sst_id();
        let sst = builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?;
        new_ssts.push(Arc::new(sst));
        Ok(())
    }

    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        // Take a snapshot of current state
        let snapshot = {
            let state = self.state.read();
            Arc::clone(&state)
        };

        let mut merge_iter = match _task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                // Add L0 SSTs (本身就是按newest to oldest排序的)
                let l0_iter = {
                    let mut l0_iters = Vec::new();
                    for &sst_id in l0_sstables.iter() {
                        if let Some(sst) = snapshot.sstables.get(&sst_id) {
                            l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                                Arc::clone(sst),
                            )?));
                        }
                    }
                    // For L0 SSTs use MergeIterator since they may overlap
                    MergeIterator::create(l0_iters)
                };

                // Add L1 SSTs
                let l1_iter = {
                    let mut l1_ssts = Vec::new();
                    for &sst_id in l1_sstables.iter() {
                        if let Some(sst) = snapshot.sstables.get(&sst_id) {
                            l1_ssts.push(Arc::clone(sst));
                        }
                    }
                    // For L1 SSTs use ConcatIterator since they are sorted and non-overlapping
                    SstConcatIterator::create_and_seek_to_first(l1_ssts)?
                };

                // Create merge iterator
                SsTableIteratorWrapper::Merge(TwoMergeIterator::create(l0_iter, l1_iter)?)
            }
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level: _,
                lower_level_sst_ids,
                ..
            })
            | CompactionTask::Leveled(LeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level: _,
                lower_level_sst_ids,
                ..
            }) => {
                // For lower level (always use ConcatIterator as they're sorted)
                let mut lower_ssts = Vec::new();
                for &sst_id in lower_level_sst_ids {
                    if let Some(sst) = snapshot.sstables.get(&sst_id) {
                        lower_ssts.push(Arc::clone(sst));
                    }
                }
                let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;

                // For upper level
                if upper_level.is_none() {
                    // L0 -> L1 case
                    let mut l0_iters = Vec::new();
                    for &sst_id in upper_level_sst_ids {
                        if let Some(sst) = snapshot.sstables.get(&sst_id) {
                            l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                                Arc::clone(sst),
                            )?));
                        }
                    }
                    let upper_iter = MergeIterator::create(l0_iters);
                    // Create merge iterator
                    SsTableIteratorWrapper::Merge(TwoMergeIterator::create(upper_iter, lower_iter)?)
                } else {
                    // Ln -> L(n+1) case
                    let mut upper_ssts = Vec::new();
                    for &sst_id in upper_level_sst_ids {
                        if let Some(sst) = snapshot.sstables.get(&sst_id) {
                            upper_ssts.push(Arc::clone(sst));
                        }
                    }
                    let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_ssts)?;
                    // Create merge iterator
                    SsTableIteratorWrapper::Two(TwoMergeIterator::create(upper_iter, lower_iter)?)
                }
            }
            CompactionTask::Tiered(task) => {
                // Create iterators for all tiers
                let mut tier_iters = Vec::new();

                // Process each tier in order
                for (_, tier_ssts) in &task.tiers {
                    // Create iterators for current tiers
                    let mut tier_ssts_refs = Vec::new();
                    for &sst_id in tier_ssts {
                        if let Some(sst) = snapshot.sstables.get(&sst_id) {
                            tier_ssts_refs.push(Arc::clone(sst));
                        }
                    }

                    // Each tier should use ConcatIterator since SSTs within a tier are sorted and non-overlapping
                    if !tier_ssts_refs.is_empty() {
                        let iter = SstConcatIterator::create_and_seek_to_first(tier_ssts_refs)?;
                        tier_iters.push(Box::new(iter));
                    }
                }

                // Create a merge iterator combining all tier iterators
                // Since tiers may overlap with each other, we use MergeIterator
                SsTableIteratorWrapper::All(MergeIterator::create(tier_iters))
            }
        };

        // Create new SSTs with compacted data
        let mut new_ssts = Vec::new();
        let mut current_builder = SsTableBuilder::new(self.options.block_size);
        let mut current_size = 0;

        while merge_iter.is_valid() {
            // Only add the key-value pair if it's not a delete marker
            if !merge_iter.value().is_empty() {
                // Add key-value pair to current builder
                current_builder.add(merge_iter.key(), merge_iter.value());
                current_size += merge_iter.key().len() + merge_iter.value().len();
            }

            // If builder exceeds target size, flush it
            if current_size >= self.options.target_sst_size {
                self.seal_sst(current_builder, &mut new_ssts)?;

                // Start new builder
                current_builder = SsTableBuilder::new(self.options.block_size);
                current_size = 0;
            }

            merge_iter.next()?;
        }

        // Flush last SST if it contains any data
        if current_size > 0 {
            self.seal_sst(current_builder, &mut new_ssts)?;
        }

        Ok(new_ssts)
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        // Collect SSTs to compact under read lock
        let (l0_sstables, l1_sstables) = {
            let state = self.state.read();
            (
                state.l0_sstables.clone(),
                // Get L1 SSTs from levels[0], as index 0 represents L1
                state
                    .levels
                    .first()
                    .map(|(_, ssts)| ssts.clone())
                    .unwrap_or_default(),
            )
        };

        // Create and execute compaction task
        let task = CompactionTask::ForceFullCompaction {
            l0_sstables,
            l1_sstables,
        };

        println!("force full compaction: {:?}", task);
        let new_ssts = self.compact(&task)?;

        // Update state under write lock with state lock protection
        {
            let _state_lock = self.state_lock.lock();
            let mut state = self.state.write();
            let mut new_state = state.as_ref().clone();

            // Need to consider new L0 files produced when compaction is going on
            // Keep L0 SSTs that were created after we started compaction
            if let CompactionTask::ForceFullCompaction { l0_sstables, .. } = &task {
                let old_l0_set: std::collections::HashSet<_> =
                    l0_sstables.iter().copied().collect();
                new_state
                    .l0_sstables
                    .retain(|sst_id| !old_l0_set.contains(sst_id));
            } else {
                new_state.l0_sstables.clear();
            }

            // Update L1 (at index 0 in levels) with new SSTs
            if let Some((_, l1_ssts)) = new_state.levels.get_mut(0) {
                l1_ssts.clear();
                l1_ssts.extend(new_ssts.iter().map(|sst| sst.sst_id()));
            } else {
                // If L1 doesn't exist, create it
                new_state
                    .levels
                    .push((1, new_ssts.iter().map(|sst| sst.sst_id()).collect()));
            }

            // Update SSTable map with new SSTs
            for sst in new_ssts {
                new_state.sstables.insert(sst.sst_id(), sst);
            }

            *state = Arc::new(new_state);
            println!(
                "force full compaction done, new SSTs: {:?}",
                state.levels.first().map(|(_, ssts)| ssts)
            );
        }

        // Sync directory after state update
        self.sync_dir()?;
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        // Take a snapshot of current state
        let snapshot = {
            let state = self.state.read();
            Arc::clone(&state)
        };

        // 1. Generate compaction task
        let task = match self
            .compaction_controller
            .generate_compaction_task(&snapshot)
        {
            Some(task) => task,
            None => return Ok(()), // No compaction needed
        };
        // print LSM structure after compaction
        self.dump_structure();
        println!("trigger compaction: {:?}", task);

        // 2. Run compaction
        let new_ssts = self.compact(&task)?;

        // 3. Update LSM state with compaction results
        {
            let _state_lock = self.state_lock.lock();
            let mut snapshot = self.state.read().as_ref().clone();

            // First insert new SSTs. In LeveledCompactionTask we need first_keys in sst.
            for sst in &new_ssts {
                snapshot.sstables.insert(sst.sst_id(), Arc::clone(sst));
            }
            // Then Apply compaction result and get removed SSTs
            let (snapshot, removed_sst_ids) = self.compaction_controller.apply_compaction_result(
                &snapshot,
                &task,
                &new_ssts.iter().map(|sst| sst.sst_id()).collect::<Vec<_>>(),
                false,
            );

            // // Add new SSTs to the state
            // let mut final_state = snapshot;
            // for sst in new_ssts {
            //     final_state.sstables.insert(sst.sst_id(), sst);
            // }

            // Update state atomically
            let mut state = self.state.write();
            *state = Arc::new(new_state);

            println!("compaction done, removed SSTs: {:?}", removed_sst_ids);
        }

        // Sync directory after state update
        self.sync_dir()?;

        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        // Check memtable count in a separate scope to drop the read lock early
        let should_flush = {
            let state = self.state.read();
            state.imm_memtables.len() >= self.options.num_memtable_limit
        };

        // If total memtables exceed limit, flush the earliest one
        if should_flush {
            self.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
