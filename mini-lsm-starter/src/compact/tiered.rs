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
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if snapshot.levels.is_empty() || snapshot.levels.len() < self.options.num_tiers {
            return None;
        }


        // Calculate space amplification ratio
        let mut all_except_last_size = 0u64;
        let mut last_level_size = 0u64;

        for (i, (_, sst_ids)) in snapshot.levels.iter().enumerate() {
            let level_size = sst_ids.len() as u64;
            if i == snapshot.levels.len() - 1 {
                last_level_size = level_size;
            } else {
                all_except_last_size += level_size;
            }
        }

        if last_level_size == 0 {
            return None;
        }

        let ratio = all_except_last_size as f64 / last_level_size as f64;
        let threshold = self.options.max_size_amplification_percent as f64 * 0.01;

        if ratio >= threshold && snapshot.levels.len() > 1 {
            // Trigger full compaction: compact all tiers
            // ONLY if we have more than 1 tier (avoid re-compacting just-created output)
            println!("compaction triggered by space amplification ratio: {}", (ratio * 100.0).round() as usize);
            let tiers = snapshot.levels.clone();
            Some(TieredCompactionTask {
                tiers,
                bottom_tier_included: true,
            })
        } else {
            None
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut levels = snapshot.levels.clone();
        let mut to_remove = Vec::new();

        if task.bottom_tier_included && task.tiers.len() > 1 {
            // Full compaction: merge all tiers into a NEW tier with a fresh level ID
            
            // Find the next available level ID (max + 1)
            let max_level_id = levels.iter()
                .map(|(id, _)| *id)
                .max()
                .unwrap_or(0);
            let new_tier_id = max_level_id + 1;
            
            // Remove all tiers involved in the compaction and collect files to remove
            for (tier_id, old_sst_ids) in &task.tiers {
                if let Some(pos) = levels.iter().position(|(id, _)| id == tier_id) {
                    to_remove.extend_from_slice(old_sst_ids);
                    levels.remove(pos);
                }
            }
            
            // Insert the new merged tier with the fresh ID
            levels.insert(0, (new_tier_id, output.to_vec()));
        } else {
            // Partial compaction: replace files in existing tier
            for (tier_id, old_sst_ids) in &task.tiers {
                if let Some(pos) = levels.iter().position(|(id, _)| id == tier_id) {
                    // Replace the SSTs in this tier
                    levels[pos].1 = output.to_vec();
                    to_remove.extend_from_slice(old_sst_ids);
                }
            }
        }

        let new_state = LsmStorageState {
            memtable: snapshot.memtable.clone(),
            imm_memtables: snapshot.imm_memtables.clone(),
            l0_sstables: snapshot.l0_sstables.clone(),
            levels,
            sstables: snapshot.sstables.clone(),
        };

        (new_state, to_remove)
    }
}
