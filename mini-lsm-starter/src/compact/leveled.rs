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

    /// Returns the lower level of two given levels.
    fn lower_level(&self, upper: usize) -> usize {
        upper + 1
    }

    /// Compute target size for each level
    fn compute_level_target_size(&self, snapshot: &LsmStorageState) -> Vec<usize> {
        let base_bytes = self.options.base_level_size_mb * 1024 * 1024;
        let max_levels = self.options.max_levels;
        let mut target_sizes = vec![0; max_levels]; // target_sizes[i] is for level i+1

        // Calculate total size of bottom level (L_max)
        let bottom_level_idx = max_levels - 1;
        let bottom_level_size = if let Some((_, sst_ids)) = snapshot.levels.get(bottom_level_idx) {
            sst_ids
                .iter()
                .filter_map(|id| snapshot.sstables.get(id))
                .map(|sst| sst.table_size())
                .sum::<u64>() as usize
        } else {
            0
        };

        if bottom_level_size <= base_bytes {
            // Bottom level hasn't reached base size yet, only bottom level has target
            target_sizes[bottom_level_idx] = base_bytes;
        } else {
            // Bottom level reached base size, compute targets from bottom to top
            let mut current_size = bottom_level_size;
            let mut level_idx = bottom_level_idx;

            while current_size > 0 {
                if level_idx == 0 {
                    // 如果 level_idx 已经到达 L0（或者你定义的最低有效层）
                    target_sizes[level_idx] = current_size; // 设置 L0 的目标大小
                    break; // 达到最顶层，退出循环
                }

                // Calculate target for this level
                if current_size > base_bytes {
                    // If target size is >= base_bytes, set it and continue to higher level
                    target_sizes[level_idx] = current_size;
                    // Calculate target for next higher level
                    current_size = (current_size as f64 / self.options.level_size_multiplier as f64)
                        .ceil() as usize;
                    level_idx -= 1; // 递减到上一层
                } else {
                    // If target size would be < base_bytes, set it for this level and stop
                    // This ensures at most one level has a target size below base_bytes
                    target_sizes[level_idx] = current_size;
                    break; // 达到条件，退出循环
                }
            }
        }

        target_sizes
    }

    /// Get level size in bytes
    fn get_level_size(&self, level: usize, snapshot: &LsmStorageState) -> usize {
        if level == 0 {
            // L0 size - sum of all L0 SSTs
            snapshot
                .l0_sstables
                .iter()
                .filter_map(|id| snapshot.sstables.get(id))
                .map(|sst| sst.table_size())
                .sum::<u64>() as usize
        } else {
            // L1+ size
            if let Some((_, sst_ids)) = snapshot.levels.get(level - 1) {
                sst_ids
                    .iter()
                    .filter_map(|id| snapshot.sstables.get(id))
                    .map(|sst| sst.table_size())
                    .sum::<u64>() as usize
            } else {
                0
            }
        }
    }

    /// Find overlapping SSTs in the lower level with the given SSTs
    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        upper_level_sst_ids: &[usize],
        lower_level: usize,
    ) -> Vec<usize> {
        if upper_level_sst_ids.is_empty() {
            return Vec::new();
        }

        // Get the key range from upper level SSTs
        let mut min_key = None;
        let mut max_key = None;

        for &sst_id in upper_level_sst_ids {
            if let Some(sst) = snapshot.sstables.get(&sst_id) {
                let first_key = sst.first_key();
                let last_key = sst.last_key();

                if min_key.is_none() || first_key.as_key_slice() < min_key.unwrap() {
                    min_key = Some(first_key.as_key_slice());
                }
                if max_key.is_none() || last_key.as_key_slice() > max_key.unwrap() {
                    max_key = Some(last_key.as_key_slice());
                }
            }
        }

        if min_key.is_none() {
            return Vec::new();
        }

        // Find overlapping SSTs in lower level
        let mut overlapping_ssts = Vec::new();

        if let Some((_, sst_ids)) = snapshot.levels.get(lower_level - 1) {
            // Query L1+ SSTs
            for &sst_id in sst_ids {
                if let Some(sst) = snapshot.sstables.get(&sst_id) {
                    let sst_first = sst.first_key().as_key_slice();
                    let sst_last = sst.last_key().as_key_slice();

                    // Check if ranges overlap
                    if sst_last >= min_key.unwrap() && sst_first <= max_key.unwrap() {
                        overlapping_ssts.push(sst_id);
                    }
                }
            }
        }

        overlapping_ssts
    }

    /// Find the level with highest compaction priority based on current_size / target_size ratio
    fn find_level_to_compact(&self, snapshot: &LsmStorageState) -> Option<(usize, usize)> {
        let target_sizes = self.compute_level_target_size(snapshot);

        // L0 compaction has top priority - trigger when we have enough SSTs
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            // Find the target level: the lowest level with target_size > 0
            // This avoids compacting across empty levels by skipping them
            for level in 1..=self.options.max_levels {
                if target_sizes[level - 1] > 0 {
                    return Some((0, level));
                }
            }
            // If no levels have target_size > 0, compact to L1
            return Some((0, 1));
        }

        // For L1+ levels, find the level with highest priority ratio (current_size / target_size)
        let mut best_priority = 0.0;
        let mut best_level = None;

        for level in 1..=self.options.max_levels {
            let level_size = self.get_level_size(level, snapshot);
            let target_size = target_sizes[level - 1];

            if target_size > 0 && level_size > target_size {
                let priority = level_size as f64 / target_size as f64;
                if priority > best_priority {
                    best_priority = priority;
                    best_level = Some(level);
                }
            }
        }

        best_level.map(|level| (level, level + 1))
    }

    /// Pick SSTs for compaction from the given level based on size or other strategies
    /// For L1+, always select exactly ONE SST (the oldest based on ID)
    fn pick_ssts_for_compaction(
        &self,
        snapshot: &LsmStorageState,
        level: usize,
        target_level: usize,
    ) -> Vec<usize> {
        if level == 0 {
            // For L0, we need to select SSTs that might overlap
            // Return first few SSTs (based on trigger)
            snapshot
                .l0_sstables
                .iter()
                .take(self.options.level0_file_num_compaction_trigger)
                .cloned()
                .collect()
        } else if let Some((_, sst_ids)) = snapshot.levels.get(level - 1) {
            // For L1+, pick exactly ONE SST - the oldest one (smallest ID)
            if let Some(&oldest_sst_id) = sst_ids.iter().min() {
                vec![oldest_sst_id]
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let (upper_level, lower_level) = self.find_level_to_compact(snapshot)?;

        let upper_level_sst_ids = self.pick_ssts_for_compaction(snapshot, upper_level, lower_level);
        if upper_level_sst_ids.is_empty() {
            return None;
        }

        // Find overlapping SSTs in lower level
        let lower_level_sst_ids =
            self.find_overlapping_ssts(snapshot, &upper_level_sst_ids, lower_level);

        let is_bottom_level = lower_level == self.options.max_levels;

        Some(LeveledCompactionTask {
            upper_level: if upper_level == 0 {
                None
            } else {
                Some(upper_level)
            },
            upper_level_sst_ids,
            lower_level,
            lower_level_sst_ids,
            is_lower_level_bottom_level: is_bottom_level,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        _in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let to_remove: Vec<usize> = task
            .upper_level_sst_ids
            .iter()
            .chain(task.lower_level_sst_ids.iter())
            .cloned()
            .collect();

        let mut new_state = LsmStorageState {
            memtable: snapshot.memtable.clone(),
            imm_memtables: snapshot.imm_memtables.clone(),
            l0_sstables: snapshot.l0_sstables.clone(),
            levels: snapshot.levels.clone(),
            sstables: snapshot.sstables.clone(),
        };

        // Remove input SSTs and add output SSTs

        if task.upper_level.is_none() {
            // L0 compaction
            new_state
                .l0_sstables
                .retain(|id| !task.upper_level_sst_ids.contains(id));
        } else if let Some(upper_level) = task.upper_level {
            // L1+ compaction
            if let Some((_, level_ssts)) = new_state.levels.get_mut(upper_level - 1) {
                level_ssts.retain(|id| !task.upper_level_sst_ids.contains(id));
            }
        }

        // Remove input SSTs from lower level
        if task.lower_level >= 1
            && let Some((_, level_ssts)) = new_state.levels.get_mut(task.lower_level - 1)
        {
            let insert_pos = level_ssts
                .iter()
                .position(|id| task.lower_level_sst_ids.contains(id))
                .unwrap_or(level_ssts.len());
            level_ssts.retain(|id| !task.lower_level_sst_ids.contains(id));
            let insert_pos = insert_pos.min(level_ssts.len());
            level_ssts.splice(insert_pos..insert_pos, output.iter().cloned());
        }

        (new_state, to_remove)
    }
}
