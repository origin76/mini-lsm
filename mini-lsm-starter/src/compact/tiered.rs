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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

        if last_level_size != 0 {
            let ratio = all_except_last_size as f64 / last_level_size as f64;
            let threshold = self.options.max_size_amplification_percent as f64 * 0.01;

            if ratio >= threshold && snapshot.levels.len() > 1 {
                // Trigger full compaction: compact all tiers
                // ONLY if we have more than 1 tier (avoid re-compacting just-created output)
                println!(
                    "compaction triggered by space amplification ratio: {}",
                    (ratio * 100.0).round() as usize
                );
                let tiers = snapshot.levels.clone();
                return Some(TieredCompactionTask {
                    tiers,
                    bottom_tier_included: true,
                });
            }
        }

        // --- 修正大小比触发器逻辑 (从最新层往最旧层计算, previous 指的是比当前层更新的层) ---
        // 假设 snapshot.levels[0] 是最新层, snapshot.levels[len-1] 是最旧层
        // 循环从第二层开始 (索引 1) 到最旧的层 (len-1)。
        // 索引 0 (最新层) 无法有 "previous" (更新) 的层来比较。
        let threshold = (100 + self.options.size_ratio) as f64 * 0.01;
        for i in 1..snapshot.levels.len() {
            // i 从 1 开始，代表 Tier 2, Tier 1...
            let current_tier_idx = i; // 当前正在检查的层 (例如，Tier 2 对应索引 1)
            let current_tier_size = snapshot.levels[current_tier_idx].1.len() as f64;

            // "previous tiers" 是指所有比 current_tier_idx 更新的层 (索引 0 到 current_tier_idx - 1)
            let newer_tiers_sum: f64 = snapshot.levels[0..current_tier_idx]
                .iter()
                .map(|(_, sst_ids)| sst_ids.len() as f64)
                .sum();

            if newer_tiers_sum > 0.0 {
                let ratio = current_tier_size / newer_tiers_sum; // 旧层 / 新层之和

                if ratio > threshold {
                    // 如果触发，这意味着 `current_tier_idx` 这一层的数据量相对于其“更新的邻居”积累过多。
                    // 按照您最开始的描述 "我们将压缩除当前层之外的所有先前层"
                    // 这里的 "先前层" (previous tiers) 就是 `snapshot.levels[0..current_tier_idx]`
                    // 也就是所有比 `current_tier_idx` 更新的层。

                    let tiers_to_merge_count = current_tier_idx; // 数量就是索引 `i`

                    if tiers_to_merge_count >= self.options.min_merge_width {
                        println!(
                            "compaction triggered by size ratio: {} (current_tier_idx={}) / {} (newer_tiers_sum) at tier index {}, merging {} tiers",
                            current_tier_size as usize,
                            current_tier_idx,
                            newer_tiers_sum as usize,
                            current_tier_idx,
                            tiers_to_merge_count
                        );

                        // 压缩所有比当前层更新的层 (snapshot.levels[0] 到 snapshot.levels[current_tier_idx - 1])
                        let tiers: Vec<(usize, Vec<usize>)> =
                            snapshot.levels[0..current_tier_idx].to_vec();

                        return Some(TieredCompactionTask {
                            tiers,
                            // 最底层是否包含取决于被合并的层是否包括了最旧的层。
                            // 在这个逻辑下，我们只合并 `0..current_tier_idx`。
                            // 如果 `current_tier_idx` 是 `len-1`，那么我们将合并 `0..len-1`，
                            // 此时 `snapshot.levels[len-1]` (最旧层) 没有被合并。
                            // 所以 `bottom_tier_included` 应该始终为 `false`。
                            bottom_tier_included: false,
                        });
                    }
                }
            }
        }

        if snapshot.levels.len() > 1 && self.options.max_merge_width.unwrap_or(usize::MAX) >= 2 {
            // 合并的层数不能超过当前实际存在的层数
            let tiers_to_merge_count = self
                .options
                .max_merge_width
                .unwrap_or(usize::MAX)
                .min(snapshot.levels.len());

            // 合并从最新层 (索引 0) 到 tiers_to_merge_count - 1 的所有层
            let tiers_to_be_compacted: Vec<(usize, Vec<usize>)> =
                snapshot.levels[0..tiers_to_merge_count].to_vec();

            // 判断最底层是否被包含在本次合并中
            // 如果合并的范围的最后一个索引是 snapshot.levels 的最后一个索引，那么最底层就被包含
            let bottom_tier_included = tiers_to_merge_count == snapshot.levels.len();

            println!(
                "Reduce Sorted Run triggered: merging {} tiers (from newest) to reduce tier count",
                tiers_to_merge_count
            );

            return Some(TieredCompactionTask {
                tiers: tiers_to_be_compacted,
                bottom_tier_included,
            });
        }

        // 如果所有触发器都未触发
        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let levels = snapshot.levels.clone();
        let mut to_remove_sst_ids = Vec::new();

        // 1. 收集需要物理删除的 SST ID
        for (_, old_sst_ids) in &task.tiers {
            to_remove_sst_ids.extend_from_slice(old_sst_ids);
        }

        // [修复核心]: 不要收集 Tier ID，而是收集 "SST 列表内容"
        // 因为恢复模式下，Tier ID 可能变了，但内容不会变
        let tiers_to_remove_content: Vec<&Vec<usize>> = task
            .tiers
            .iter()
            .map(|(_, ssts)| ssts) // 忽略 task 里的 ID，只看 ssts
            .collect();

        let mut new_levels_list = Vec::with_capacity(levels.len() - task.tiers.len() + 1);
        let mut insertion_point_found = false;
        let mut insertion_idx = 0;

        for (idx, (level_id, sst_ids)) in levels.into_iter().enumerate() {
            // [修复核心]: 比较当前层的 sst_ids 是否在任务的目标列表中
            // 这里使用了 contains 来比较内容 (Vec<usize> 的比较是按值比较的)
            if tiers_to_remove_content.contains(&&sst_ids) {
                // 找到了要删除的层（内容匹配上了）
                if !insertion_point_found {
                    insertion_idx = idx;
                    insertion_point_found = true;
                }
            } else {
                // 不需要删除，保留
                new_levels_list.push((level_id, sst_ids));
            }
        }

        // 3. 插入新生成的层
        // 只有当有 output 时才插入 (避免空 output 占位)
        if !output.is_empty() {
            // 建议：使用 output[0] 作为新 Tier ID，因为它全局唯一且单调递增，
            let new_tier_id = output[0];
            new_levels_list.insert(insertion_idx, (new_tier_id, output.to_vec()));
        }

        let new_state = LsmStorageState {
            memtable: snapshot.memtable.clone(),
            imm_memtables: snapshot.imm_memtables.clone(),
            l0_sstables: snapshot.l0_sstables.clone(),
            levels: new_levels_list,
            sstables: snapshot.sstables.clone(),
        };

        (new_state, to_remove_sst_ids)
    }
}
