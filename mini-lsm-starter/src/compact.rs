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
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

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
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, iterator::SsTableIterator};

#[derive(Debug, Clone, Serialize, Deserialize)]
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

pub enum CompactionIterator {
    SsTable(SsTableIterator),
    Concat(SstConcatIterator),
    Merge(MergeIterator<CompactionIterator>), // MergeIterator的内部迭代器现在是Box<CompactionIterator>
                                              // 如果MergeIterator可以接受更泛化的I: StorageIterator，那么这里可以是I
                                              // 但为了递归定义，我们直接用CompactionIterator
}

// 为 CompactionIterator 实现 StorageIterator trait
impl StorageIterator for CompactionIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&'_ self) -> KeySlice<'_> {
        match self {
            CompactionIterator::SsTable(iter) => iter.key(),
            CompactionIterator::Concat(iter) => iter.key(),
            CompactionIterator::Merge(iter) => iter.key(),
        }
    }

    fn value(&self) -> &[u8] {
        match self {
            CompactionIterator::SsTable(iter) => iter.value(),
            CompactionIterator::Concat(iter) => iter.value(),
            CompactionIterator::Merge(iter) => iter.value(),
        }
    }

    fn is_valid(&self) -> bool {
        match self {
            CompactionIterator::SsTable(iter) => iter.is_valid(),
            CompactionIterator::Concat(iter) => iter.is_valid(),
            CompactionIterator::Merge(iter) => iter.is_valid(),
        }
    }

    fn next(&mut self) -> Result<()> {
        match self {
            CompactionIterator::SsTable(iter) => iter.next(),
            CompactionIterator::Concat(iter) => iter.next(),
            CompactionIterator::Merge(iter) => iter.next(),
        }
    }
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
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let snapshot = self.state.read().clone();
                let sstables = &snapshot.sstables;
                let mut all_ids = l0_sstables.clone();
                all_ids.extend_from_slice(l1_sstables);

                // 1. 修改类型：改为存放 CompactionIterator
                let mut iterators: Vec<Box<CompactionIterator>> = Vec::new();

                for &id in &all_ids {
                    if let Some(sst) = sstables.get(&id) {
                        let iter = SsTableIterator::create_and_seek_to_first(sst.clone())?;
                        // 2. 包装：放入枚举变体 SsTable 中
                        iterators.push(Box::new(CompactionIterator::SsTable(iter)));
                    }
                }

                let merge_iter = CompactionIterator::Merge(MergeIterator::create(iterators));

                self.compact_generate_sst_from_iter(merge_iter, task.compact_to_bottom_level())
            }
            CompactionTask::Simple(task) => {
                let snapshot = self.state.read().clone();
                let sstables_map = &snapshot.sstables;

                let final_compaction_iterator: Box<CompactionIterator> =
                    if task.upper_level.is_none() {
                        // L0 -> L1
                        let mut sources_for_top_merge: Vec<Box<CompactionIterator>> = Vec::new();
                        for &id in &task.upper_level_sst_ids {
                            if let Some(sst) = sstables_map.get(&id) {
                                sources_for_top_merge.push(Box::new(CompactionIterator::SsTable(
                                    SsTableIterator::create_and_seek_to_first(sst.clone())?,
                                )));
                            }
                        }

                        let mut lower_level_ssts = Vec::new();
                        for &id in &task.lower_level_sst_ids {
                            if let Some(sst) = sstables_map.get(&id) {
                                lower_level_ssts.push(sst.clone());
                            }
                        }
                        if !lower_level_ssts.is_empty() {
                            sources_for_top_merge.push(Box::new(CompactionIterator::Concat(
                                SstConcatIterator::create_and_seek_to_first(lower_level_ssts)?,
                            )));
                        }

                        Box::new(CompactionIterator::Merge(MergeIterator::create(
                            sources_for_top_merge,
                        )))
                    } else {
                        // Ln -> Ln+1
                        let mut sources_for_top_merge: Vec<Box<CompactionIterator>> = Vec::new();
                        let mut upper_level_ssts = Vec::new();
                        for &id in &task.upper_level_sst_ids {
                            if let Some(sst) = sstables_map.get(&id) {
                                upper_level_ssts.push(sst.clone());
                            }
                        }
                        if !upper_level_ssts.is_empty() {
                            sources_for_top_merge.push(Box::new(CompactionIterator::Concat(
                                SstConcatIterator::create_and_seek_to_first(upper_level_ssts)?,
                            )));
                        }

                        let mut lower_level_ssts = Vec::new();
                        for &id in &task.lower_level_sst_ids {
                            if let Some(sst) = sstables_map.get(&id) {
                                lower_level_ssts.push(sst.clone());
                            }
                        }
                        if !lower_level_ssts.is_empty() {
                            sources_for_top_merge.push(Box::new(CompactionIterator::Concat(
                                SstConcatIterator::create_and_seek_to_first(lower_level_ssts)?,
                            )));
                        }

                        Box::new(CompactionIterator::Merge(MergeIterator::create(
                            sources_for_top_merge,
                        )))
                    };

                self.compact_generate_sst_from_iter(
                    *final_compaction_iterator,
                    task.is_lower_level_bottom_level,
                )
            }
            CompactionTask::Tiered(task) => {
                let snapshot = self.state.read().clone();
                let sstables_map = &snapshot.sstables;

                // 1. 这里的类型必须变为 Box<CompactionIterator> 以适配 MergeIterator<CompactionIterator>
                let mut iterators: Vec<Box<CompactionIterator>> = Vec::new();

                for (_, sst_ids) in &task.tiers {
                    for &sst_id in sst_ids {
                        if let Some(sst) = sstables_map.get(&sst_id) {
                            // 创建基础迭代器
                            let iter = SsTableIterator::create_and_seek_to_first(sst.clone())?;

                            // 关键修改：
                            // 1. 用 CompactionIterator::SsTable 包裹
                            // 2. 用 Box::new 装箱
                            iterators.push(Box::new(CompactionIterator::SsTable(iter)));
                        }
                    }
                }

                // 2. 创建 MergeIterator，此时它的类型是 MergeIterator<CompactionIterator>
                // 3. 再将其包裹在 CompactionIterator::Merge 变体中，变成单一的 CompactionIterator 类型
                let final_iter = CompactionIterator::Merge(MergeIterator::create(iterators));

                // 现在可以直接传入辅助函数了
                self.compact_generate_sst_from_iter(final_iter, task.bottom_tier_included)
            }
            CompactionTask::Leveled(task) => {
                let snapshot = self.state.read().clone();
                let sstables_map = &snapshot.sstables;

                let final_compaction_iterator: Box<CompactionIterator> =
                    if task.upper_level.is_none() {
                        let mut sources_for_top_merge: Vec<Box<CompactionIterator>> = Vec::new();
                        for &id in &task.upper_level_sst_ids {
                            if let Some(sst) = sstables_map.get(&id) {
                                sources_for_top_merge.push(Box::new(CompactionIterator::SsTable(
                                    SsTableIterator::create_and_seek_to_first(sst.clone())?,
                                )));
                            }
                        }
                        let mut lower_level_ssts = Vec::new();
                        for &id in &task.lower_level_sst_ids {
                            if let Some(sst) = sstables_map.get(&id) {
                                lower_level_ssts.push(sst.clone());
                            }
                        }
                        if !lower_level_ssts.is_empty() {
                            sources_for_top_merge.push(Box::new(CompactionIterator::Concat(
                                SstConcatIterator::create_and_seek_to_first(lower_level_ssts)?,
                            )));
                        }
                        Box::new(CompactionIterator::Merge(MergeIterator::create(
                            sources_for_top_merge,
                        )))
                    } else {
                        let mut sources_for_top_merge: Vec<Box<CompactionIterator>> = Vec::new();
                        let mut upper_level_ssts = Vec::new();
                        for &id in &task.upper_level_sst_ids {
                            if let Some(sst) = sstables_map.get(&id) {
                                upper_level_ssts.push(sst.clone());
                            }
                        }
                        if !upper_level_ssts.is_empty() {
                            sources_for_top_merge.push(Box::new(CompactionIterator::Concat(
                                SstConcatIterator::create_and_seek_to_first(upper_level_ssts)?,
                            )));
                        }
                        let mut lower_level_ssts = Vec::new();
                        for &id in &task.lower_level_sst_ids {
                            if let Some(sst) = sstables_map.get(&id) {
                                lower_level_ssts.push(sst.clone());
                            }
                        }
                        if !lower_level_ssts.is_empty() {
                            sources_for_top_merge.push(Box::new(CompactionIterator::Concat(
                                SstConcatIterator::create_and_seek_to_first(lower_level_ssts)?,
                            )));
                        }
                        Box::new(CompactionIterator::Merge(MergeIterator::create(
                            sources_for_top_merge,
                        )))
                    };

                self.compact_generate_sst_from_iter(
                    *final_compaction_iterator,
                    task.is_lower_level_bottom_level,
                )
            }
        }
    }

    // 核心 MVCC 压缩逻辑
    fn compact_generate_sst_from_iter(
        &self,
        mut iter: CompactionIterator,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut builder = SsTableBuilder::new(self.options.block_size);
        let mut new_ssts = Vec::new();
        let mut last_user_key: Vec<u8> = Vec::new();
        // Track the last key added to the current builder (for SST splitting)
        let mut last_key_in_builder: Vec<u8> = Vec::new();

        // Get watermark for MVCC version cleanup
        let watermark = self.mvcc().watermark();
        // Track if we've seen a version at or below watermark for the current key
        let mut has_version_at_or_below_watermark = false;

        while iter.is_valid() {
            let key = iter.key();
            let value = iter.value();
            let ts = key.ts();
            let current_user_key = key.key_ref();

            // Check if this is a new user key
            if current_user_key != last_user_key {
                // Reset tracking for new key
                has_version_at_or_below_watermark = false;
                last_user_key = current_user_key.to_vec();
            }

            // Determine if we should keep this version
            let should_keep = if ts > watermark {
                // Version above watermark: always keep
                true
            } else {
                // Version at or below watermark
                if has_version_at_or_below_watermark {
                    // Already seen a version at or below watermark for this key, skip
                    false
                } else {
                    // First (latest) version at or below watermark
                    has_version_at_or_below_watermark = true;
                    // If it's a delete marker and we're at bottom level, we can remove it
                    !(compact_to_bottom_level && value.is_empty())
                }
            };

            if should_keep {
                // Check if we need to split to a new SST
                // Only split when we have a different user key from the last key in the builder
                if builder.estimated_size() >= self.options.target_sst_size
                    && !last_key_in_builder.is_empty()
                    && current_user_key != last_key_in_builder.as_slice()
                {
                    let sst_id = self.next_sst_id();
                    let path = self.path_of_sst(sst_id);
                    let sst = builder.build(sst_id, Some(self.block_cache.clone()), path)?;
                    new_ssts.push(Arc::new(sst));
                    builder = SsTableBuilder::new(self.options.block_size);
                    last_key_in_builder.clear();
                }

                builder.add(key, value);
                last_key_in_builder = current_user_key.to_vec();
            }

            iter.next()?;
        }

        if !builder.is_empty() {
            let sst_id = self.next_sst_id();
            let path = self.path_of_sst(sst_id);
            let sst = builder.build(sst_id, Some(self.block_cache.clone()), path)?;
            new_ssts.push(Arc::new(sst));
        }

        Ok(new_ssts)
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let curr_state = {
            let read_guard = self.state.read();
            read_guard.clone()
        };

        let mut l0_sstables = {
            let read_guard = self.state.read();
            read_guard.l0_sstables.clone()
        };

        let mut l1_sstables = {
            let read_guard = self.state.read();
            read_guard.levels.first().unwrap().1.clone()
        };

        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };

        let mut old_ssts = Vec::new();
        old_ssts.append(&mut l0_sstables);
        old_ssts.append(&mut l1_sstables);

        let new_ssts = self.compact(&task)?;
        let new_sst_ids: Vec<_> = new_ssts.iter().map(|i| i.sst_id()).collect();

        // Sync directory after writing new SST files
        self.sync_dir()?;

        // Write manifest record
        if let Some(manifest) = &self.manifest {
            manifest.add_record_when_init(crate::manifest::ManifestRecord::Compaction(
                task.clone(),
                new_sst_ids.clone(),
            ))?;
        }

        // Swap the state atomically
        {
            let mut state = self.state.write(); // Lock state for exclusive access
            let old_l0_sstables = curr_state.l0_sstables.clone();
            let mut new_l0_sstables = state.l0_sstables.clone();
            new_l0_sstables.retain(|sst_id| !old_l0_sstables.contains(sst_id));
            let mut new_sstables = state.sstables.clone();

            for i in new_ssts {
                new_sstables.insert(i.sst_id(), i);
            }
            for &i in &old_ssts {
                new_sstables.remove(&i);
            }

            let new_state = LsmStorageState {
                memtable: curr_state.memtable.clone(),
                l0_sstables: new_l0_sstables, // update with new data as needed
                levels: vec![(1, new_sst_ids)], // update levels atomically
                imm_memtables: curr_state.imm_memtables.clone(),
                sstables: new_sstables,
            };
            *state = Arc::new(new_state); // Atomic replacement of the state
        }

        for i in old_ssts {
            std::fs::remove_file(self.path_of_sst(i))?;
        }

        Ok(())
    }

    pub fn trigger_compaction(&self) -> Result<()> {
        match self.is_in_compact.compare_exchange(
            false,             // 期望当前值为 `false`
            true,              // 成功时设置为 `true`
            Ordering::Acquire, // 成功时的内存顺序：获取语义，确保在标志设置前所有内存操作可见
            Ordering::Relaxed, // 失败时的内存顺序：只读，无需强同步
        ) {
            Ok(_) => {}
            Err(_) => {
                return Ok(());
            }
        }
        let start_time = Instant::now(); // 可选：测量耗时

        // Compaction 任务的实际执行逻辑
        let snapshot = self.state.read().clone();
        let Some(task) = self
            .compaction_controller
            .generate_compaction_task(&snapshot)
        else {
            self.is_in_compact.store(false, Ordering::Release);
            return Ok(());
        };

        let new_ssts = self.compact(&task)?;
        let output_ids: Vec<_> = new_ssts.iter().map(|s| s.sst_id()).collect();

        // Sync directory after writing new SST files
        self.sync_dir()?;

        // Write manifest record
        if let Some(manifest) = &self.manifest {
            manifest.add_record_when_init(crate::manifest::ManifestRecord::Compaction(
                task.clone(),
                output_ids.clone(),
            ))?;
        }

        let mut state_guard = self.state.write();
        let (mut new_state, to_remove) = self.compaction_controller.apply_compaction_result(
            &state_guard,
            &task,
            &output_ids,
            false,
        );
        for sst in new_ssts {
            new_state.sstables.insert(sst.sst_id(), sst);
        }
        for &id in &to_remove {
            new_state.sstables.remove(&id);
        }
        // Sort levels if leveled or simple compaction
        if matches!(
            self.compaction_controller,
            CompactionController::Leveled(_) | CompactionController::Simple(_)
        ) {
            for (_, level_ssts) in &mut new_state.levels {
                level_ssts.sort_by_key(|&id| {
                    new_state
                        .sstables
                        .get(&id)
                        .unwrap()
                        .first_key()
                        .as_key_slice()
                });
            }
        }
        self.verify_lsm_invariants(&new_state);
        *state_guard = Arc::new(new_state);
        std::mem::drop(state_guard);
        let duration = start_time.elapsed(); // 可选：测量耗时

        // 在所有逻辑状态更新和文件删除前，确保标志被重置
        // 物理删除文件
        for id in to_remove {
            let res = std::fs::remove_file(self.path_of_sst(id));
            if let Err(e) = res {
                println!("remove fail {} {}", id, e); // 使用 eprintln
            }
        }

        // 所有操作完成后，原子地将 `is_in_compact` 设置回 `false`，释放槽位。
        // Ordering::Release 确保所有在此之前对内存的写操作都在标志释放之前完成。
        self.is_in_compact.store(false, Ordering::Release);

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
        let should_flush = {
            let state = self.state.read();
            let total_memtables = 1 + state.imm_memtables.len();
            total_memtables > self.options.num_memtable_limit && !state.imm_memtables.is_empty()
        };

        if should_flush {
            self.force_flush_next_imm_memtable()
        } else {
            Ok(())
        }
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
