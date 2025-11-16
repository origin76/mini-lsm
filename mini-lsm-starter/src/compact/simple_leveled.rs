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

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        // L0 trigger
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: snapshot.levels[0].1.clone(),
                is_lower_level_bottom_level: 1 == self.options.max_levels,
            });
        }
        // Size ratio trigger
        for level in 0..self.options.max_levels - 1 {
            let upper_len = snapshot.levels[level].1.len();
            let lower_len = snapshot.levels[level + 1].1.len();
            if upper_len > 0 {
                let ratio = if lower_len == 0 {
                    0.0
                } else {
                    (lower_len as f64 / upper_len as f64) * 100.0
                };
                if ratio < self.options.size_ratio_percent as f64 {
                    return Some(SimpleLeveledCompactionTask {
                        upper_level: Some(level + 1),
                        upper_level_sst_ids: snapshot.levels[level].1.clone(),
                        lower_level: level + 2,
                        lower_level_sst_ids: snapshot.levels[level + 1].1.clone(),
                        is_lower_level_bottom_level: level + 2 == self.options.max_levels,
                    });
                }
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
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let to_remove = task
            .upper_level_sst_ids
            .iter()
            .chain(task.lower_level_sst_ids.iter())
            .cloned()
            .collect::<Vec<_>>();

        let mut levels = snapshot.levels.clone();

        // Update L0 if applicable
        let new_l0_sstables = if task.upper_level.is_none() {
            snapshot
                .l0_sstables
                .iter()
                .filter(|id| !task.upper_level_sst_ids.contains(id))
                .cloned()
                .collect()
        } else {
            snapshot.l0_sstables.clone()
        };

        // Update levels
        for (idx, (_, level_ssts)) in levels.iter_mut().enumerate() {
            let level = idx + 1; // levels[0] is L1
            if Some(level) == task.upper_level {
                level_ssts.retain(|id| !task.upper_level_sst_ids.contains(id));
            }
            if level == task.lower_level {
                level_ssts.retain(|id| !task.lower_level_sst_ids.contains(id));
                level_ssts.extend_from_slice(output);
            }
        }

        let new_state = LsmStorageState {
            memtable: snapshot.memtable.clone(),
            imm_memtables: snapshot.imm_memtables.clone(),
            l0_sstables: new_l0_sstables,
            levels,
            sstables: snapshot.sstables.clone(),
        };

        (new_state, to_remove)
    }
}
