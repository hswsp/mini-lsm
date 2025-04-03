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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        // If no SSTs, no compaction needed
        if _snapshot.levels.is_empty() {
            return None;
        }
        // If less than num_tiers, no compaction needed
        if _snapshot.levels.len() < self.options.num_tiers {
            return None;
        }
        println!("current size_ratio is {}",self.options.size_ratio);
        // Calculate prefix sum array for tier sizes
        let mut prefix_sums: Vec<usize> = Vec::with_capacity(_snapshot.levels.len() + 1);
        prefix_sums.push(0); // Start with 0 for easier calculations

        let mut running_sum = 0;       
        for (_, ssts) in _snapshot.levels.iter() {
            running_sum += ssts.len();
            prefix_sums.push(running_sum);
        }

        // First check: space amplification ratio
        let last_tier_size = prefix_sums[_snapshot.levels.len()] - prefix_sums[_snapshot.levels.len() - 1];
        let other_tiers_size = prefix_sums[_snapshot.levels.len() - 1]; // Sum of all tiers except last

        let last_tier_size = if last_tier_size == 0 { 1 } else { last_tier_size };
        let space_amp_ratio = other_tiers_size * 100 / last_tier_size;
        if  space_amp_ratio >= self.options.max_size_amplification_percent {
            println!(
                "tiered compaction triggered by space amplification {}: current space amplification ratio {}",
                self.options.max_size_amplification_percent,
                space_amp_ratio
            );
            return Some(TieredCompactionTask {
                tiers: _snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        // Second check: compaction triggered by size ratio
        let size_ratio_trigger = (100.0 + self.options.size_ratio as f64) / 100.0;
        for id in 1.._snapshot.levels.len() {
            let current_tier_size = prefix_sums[id + 1] - prefix_sums[id];
            let prev_tiers_sum = prefix_sums[id]; // Sum of all previous tiers
            let ratio = (current_tier_size  as f64) / prev_tiers_sum as f64;
    
            if ratio > size_ratio_trigger && id >= self.options.min_merge_width {
                // excluding the current tier
                let tiers = _snapshot.levels
                                                                .iter()
                                                                .take(id)
                                                                .cloned()
                                                                .collect::<Vec<_>>();

                println!(
                    "tiered compaction triggered by size ratio {}%: current size ratio {}%", 
                    size_ratio_trigger * 100.0, 
                    ratio * 100.0);
               
                return Some(TieredCompactionTask {
                    tiers,
                    bottom_tier_included: id >= _snapshot.levels.len(), // excluding the current tier, acutally should alaways be false
                });
            }
        }

        // Third check: Reduce Sorted Runs
        let num_tiers_to_take = _snapshot
            .levels
            .len()
            .min(self.options.max_merge_width.unwrap_or(usize::MAX));

        println!(
            "tiered compaction triggered by reducing sorted runs: current {} exceeds target {}, compacting top {} tiers",
            _snapshot.levels.len(),
            self.options.num_tiers,
            num_tiers_to_take
        );

        Some(TieredCompactionTask {
            tiers: _snapshot
                .levels
                .iter()
                .take(num_tiers_to_take)
                .cloned()
                .collect::<Vec<_>>(),
            bottom_tier_included: _snapshot.levels.len() >= num_tiers_to_take,
        })
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &TieredCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        assert!(
            _snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );

        let mut new_levels: Vec<(usize, Vec<usize>)> = Vec::new();

        // Part 1: Keep new tiers that were created during compaction (they have smaller tier_id)
        let max_compacted_tier_id = _task.tiers.first().map(|(id, _)| *id).unwrap_or(0);
        new_levels.extend(
            _snapshot
                .levels
                .iter()
                .filter(|(tier_id, _)| *tier_id > max_compacted_tier_id)
                .map(|(tier_id, tier_ssts)| (*tier_id, tier_ssts.clone())),
        );


        // Part 2: Add compaction output as a new tier
        let mut tier_to_remove = _task
                                    .tiers
                                    .iter()
                                    .map(|(x, y)| (*x, y))
                                    .collect::<HashMap<_, _>>();
        // Collect SSTs that were compacted
        let mut compacted_ssts = Vec::new();

        if !_output.is_empty() {
            // Check if any SSTs in output exist in snapshot with different content
            for (tier_id, tier_ssts) in &_snapshot.levels{
                if let Some(ffiles) = tier_to_remove.remove(tier_id) {
                   // the tier should be removed
                    assert_eq!(ffiles, tier_ssts, "file changed after issuing compaction task");
                    compacted_ssts.extend(ffiles.iter().copied());
                }
            }
            if new_levels.is_empty() {
                // use the first output SST id as the level/tier id for your new sorted run
                new_levels.push((_output[0], _output.to_vec()));
            }else{
                new_levels.push((max_compacted_tier_id, _output.to_vec()));
            }     
        }
          
        // Part 3: Keep tiers below the compaction range if bottom_tier_included is false
        if !_task.bottom_tier_included {
            let min_compacted_tier_id = _task.tiers.last().map(|(id, _)| *id).unwrap_or(0);
            new_levels.extend(
                _snapshot
                    .levels
                    .iter()
                    .filter(|(tier_id, _)| *tier_id < min_compacted_tier_id)
                    .map(|(tier_id, tier_ssts)| (*tier_id, tier_ssts.clone())),
            );
        }

        // Create new state using replace
        let new_state = LsmStorageState {
            levels: new_levels,
            ..(_snapshot.clone())
        };

        (new_state, compacted_ssts)
    }
}
