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

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    // Helper function to get the actual size of the level
    fn calculate_real_size(&self, _snapshot: &LsmStorageState) -> Vec<usize> {
        let mut real_sizes = vec![0; self.options.max_levels + 1]; // +1 for 0-based indexing

        for (level, ssts) in _snapshot.levels.iter() {
            real_sizes[*level] = ssts
                .iter()
                .filter_map(|id| _snapshot.sstables.get(id))
                .map(|sst| sst.table_size())
                .sum::<u64>() as usize;
        }

        real_sizes
    }

    fn calculate_target_sizes(&self, _snapshot: &LsmStorageState) -> (Vec<usize>, Vec<usize>) {
        let real_level_size = self.calculate_real_size(_snapshot);

        let base_size = self.options.base_level_size_mb * 1024 * 1024;
        let bottom_level_size = real_level_size[self.options.max_levels];
        let mut target_sizes = vec![0; self.options.max_levels + 1]; // +1 for 0-based indexing

        // If bottom level is smaller than base size
        if bottom_level_size <= base_size {
            target_sizes[self.options.max_levels] = base_size;
            return (real_level_size, target_sizes);
        }

        // When bottom level is larger than base size
        target_sizes[self.options.max_levels] = bottom_level_size;
        let mut target = bottom_level_size;

        // Calculate backwards from bottom level
        for level in (1..self.options.max_levels).rev() {
            target /= self.options.level_size_multiplier;
            target_sizes[level] = target;

            if target < base_size {
                break; // Stop here as we found the first level below base_size
            }
        }

        (real_level_size, target_sizes)
    }

    fn find_overlapping_ssts(
        &self,
        _snapshot: &LsmStorageState,
        _sst_ids: &[usize],
        _in_level: usize,
    ) -> Vec<usize> {
        let mut overlapping = Vec::new();

        // Find the SSTs in the given level
        let level_ssts = _snapshot
            .levels
            .iter()
            .find(|(level, _)| *level == _in_level)
            .map(|(_, ssts)| ssts.clone())
            .unwrap_or_default();

        // Get key range of input SSTs using iterators
        let min_key = _sst_ids
            .iter()
            .map(|id| _snapshot.sstables[id].first_key())
            .min()
            .cloned()
            .unwrap();

        let max_key = _sst_ids
            .iter()
            .map(|id| _snapshot.sstables[id].last_key())
            .max()
            .cloned()
            .unwrap();

        // Find overlapping SSTs
        for &sst_id in &level_ssts {
            if let Some(sst) = _snapshot.sstables.get(&sst_id) {
                let first_key = sst.first_key();
                let last_key = sst.last_key();
                if !(last_key < &min_key || first_key > &max_key) {
                    overlapping.push(sst_id);
                }
            }
        }

        overlapping
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let (real_level_size, target_sizes) = self.calculate_target_sizes(_snapshot);

        // Check L0 first
        if _snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            // Decide Base Level。
            // Find the first level with non-zero target size
            for (level, _) in _snapshot.levels.iter() {
                if target_sizes[*level] > 0 {
                    println!("flush L0 SST to base level {}", *level);
                    let task = LeveledCompactionTask {
                        upper_level: None,                                  // L0
                        upper_level_sst_ids: _snapshot.l0_sstables.clone(), // L0 SST should be compacted all
                        lower_level: *level,
                        lower_level_sst_ids: self.find_overlapping_ssts(
                            _snapshot,
                            &_snapshot.l0_sstables,
                            *level,
                        ),
                        is_lower_level_bottom_level: *level == self.options.max_levels,
                    };
                    return Some(task);
                }
            }
        }

        // Calculate priorities for each level
        let mut max_priority = 0.0;
        let mut base_level = None;

        for i in 1..=self.options.max_levels {
            let target_size = target_sizes[i];
            if target_size == 0 {
                continue;
            }

            // Calculate current level size
            let current_size = real_level_size[i];

            // Decide Level Priorities
            let priority = current_size as f64 / (target_size as f64);
            if priority > 1.0 && priority > max_priority {
                max_priority = priority;
                base_level = Some(i);
            }
        }

        if let Some(level) = base_level {
            println!(
                "target level sizes: {:?}, real level sizes: {:?}, selected_level: {}",
                target_sizes
                    .iter()
                    .map(|x| format!("{:.3}MB", *x as f64 / 1024.0 / 1024.0))
                    .collect::<Vec<_>>(),
                real_level_size
                    .iter()
                    .map(|x| format!("{:.3}MB", *x as f64 / 1024.0 / 1024.0))
                    .collect::<Vec<_>>(),
                level,
            );

            // Get the oldest SST from the upper level
            let upper_ssts = &_snapshot
                .levels
                .iter()
                .find(|(l, _)| *l == level)
                .unwrap()
                .1;

            // select the oldest SST from the upper level.
            // You can know the time that the SST is produced by comparing the SST id
            let oldest_sst_id = *upper_ssts.iter().min().unwrap();

            println!(
                "compaction triggered by priority: {level} out of {:?}, select {oldest_sst_id} for compaction",
                max_priority
            );
            return Some(LeveledCompactionTask {
                upper_level: Some(level),
                upper_level_sst_ids: vec![oldest_sst_id],
                lower_level: level + 1,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    _snapshot,
                    &[oldest_sst_id],
                    level + 1,
                ),
                is_lower_level_bottom_level: level + 1 == self.options.max_levels,
            });
        }
        None
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &LeveledCompactionTask,
        _output: &[usize],
        _in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_state = _snapshot.clone();
        let mut files_to_remove = Vec::new();
// Handle upper level (L0 or Ln)
match _task.upper_level {
    Some(upper_level) => {
        new_state.levels[upper_level - 1].1.retain(|sst_id| 
            !_task.upper_level_sst_ids.contains(sst_id));
    }
    None => {
        new_state.l0_sstables.retain(|sst_id| 
            !_task.upper_level_sst_ids.contains(sst_id));
    }
}

// Handle lower level and sort if needed
let lower_idx = _task.lower_level - 1;
new_state.levels[lower_idx].1.retain(|sst_id| 
    !_task.lower_level_sst_ids.contains(sst_id));
new_state.levels[lower_idx].1.extend(_output);

// Sort by first keys if not L0 and not in recovery
if !_in_recovery && _task.lower_level != 0 {
    new_state.levels[lower_idx].1.sort_by_cached_key(|sst_id| 
        new_state.sstables[sst_id].first_key().clone()
    );
}

files_to_remove.extend(&_task.upper_level_sst_ids);
files_to_remove.extend(&_task.lower_level_sst_ids);

(new_state, files_to_remove)
    }
}
