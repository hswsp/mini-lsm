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

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    // lower level number of files / upper level number of files * 100
    // When the ratio is too low (upper level has too many files), we should trigger a compaction.
    pub size_ratio_percent: usize,
    // when the number of SSTs in L0 is larger than or equal to this number, trigger a compaction of L0 and L1
    pub max_levels: usize,
    // the number of levels (excluding L0) in the LSM tree
    pub level0_file_num_compaction_trigger: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        if self.options.max_levels == 0 {
            return None;
        }
        // Check L0 first - if we have too many L0 files, compact them with L1
        if _snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            println!(
                "compaction triggered at level 0 because L0 has {} SSTs >= {}",
                _snapshot.l0_sstables.len(),
                self.options.level0_file_num_compaction_trigger
            );
            return Some(SimpleLeveledCompactionTask {
                upper_level: None, // None means L0
                upper_level_sst_ids: _snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: _snapshot
                    .levels
                    .first()
                    .map(|(_, ssts)| ssts.clone())
                    .unwrap_or_default(),
                is_lower_level_bottom_level: self.options.max_levels == 1,
            });
        }

        // Check other levels (L1 to max_levels)
        if self.options.max_levels < 2 {
            return None;
        }

        for level in 1..self.options.max_levels {
            let (upper_files, upper_sst_ids) = {
                // upper_level is the level to be compacted
                let upper_level = _snapshot.levels.iter().find(|(lvl, _)| *lvl == level);
                (
                    upper_level.map_or(0, |(_, ssts)| ssts.len()),
                    upper_level
                        .map(|(_, ssts)| ssts.clone())
                        .unwrap_or_default(),
                )
            };

            // Find lower level SSTs once
            let lower_level = _snapshot.levels.iter().find(|(lvl, _)| *lvl == level + 1);

            let (lower_files, lower_sst_ids) = (
                lower_level.map_or(0, |(_, ssts)| ssts.len()),
                lower_level
                    .map(|(_, ssts)| ssts.clone())
                    .unwrap_or_default(),
            );

            // Check if the size ratio violates our threshold
            if upper_files > 0 && lower_files * 100 / upper_files < self.options.size_ratio_percent
            {
                println!(
                    "compaction triggered at level {} and {} with size ratio {}",
                    level,
                    level + 1,
                    lower_files * 100 / upper_files
                );

                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(level),
                    upper_level_sst_ids: upper_sst_ids,
                    lower_level: level + 1,
                    lower_level_sst_ids: lower_sst_ids,
                    is_lower_level_bottom_level: level == self.options.max_levels,
                });
            }
        }

        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &SimpleLeveledCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_state = _snapshot.c

        let mut removed_sst_ids = Vec::new();

         // Convert SST IDs to HashSet for O(1) lookup
        let upper_sst_set: HashSet<_> = _task.upper_level_sst_ids.iter().copied().collect();
        let lower_sst_set: HashSet<_> = _task.lower_level_sst_ids.iter().copied().collect();

        match _task.upper_level {
            // L0 -> L1 compaction
            None => {
                // Collect SSTs to remove
                removed_sst_ids.extend(_task.upper_level_sst_ids.iter().cloned());
                removed_sst_ids.extend(_task.lower_level_sst_ids.iter().cloned());

                // Remove compacted L0 SSTs
                new_state
                    .l0_sstables
                    .retain(|sst_id| !_task.upper_level_sst_ids.contains(sst_id));

                // Update L1 (index 0 in levels map)
                if let Some((_, ssts)) = new_state.levels.first_mut() {
                    // Keep SSTs that were not compacted
                    ssts.retain(|sst_id| !_task.lower_level_sst_ids.contains(sst_id));
                    // Add newly generated SSTs
                    ssts.extend(_output.iter().cloned());
                }
            }

            // Regular level compaction (Ln -> Ln+1)
            Some(upper_level) => {
                // Collect SSTs to remove
                removed_sst_ids.extend(_task.upper_level_sst_ids.iter().cloned());
                removed_sst_ids.extend(_task.lower_level_sst_ids.iter().cloned());

                // Update upper level (Ln)
                if let Some((_, ssts)) = new_state
                    .levels
                    .iter_mut()
                    .find(|(lvl, _)| *lvl == upper_level)
                {
                    ssts.retain(|sst_id| !_task.upper_level_sst_ids.contains(sst_id));
                }

                // Update lower level (Ln+1)
                if let Some((_, ssts)) = new_state
                    .levels
                    .iter_mut()
                    .find(|(lvl, _)| *lvl == _task.lower_level)
                {
                    // Keep SSTs that were not compacted
                    ssts.retain(|sst_id| !_task.lower_level_sst_ids.contains(sst_id));
                    // Add newly generated SSTs
                    ssts.extend(_output.iter().cloned());
                }
            }
        }

        (new_state, removed_sst_ids)
    }
}
