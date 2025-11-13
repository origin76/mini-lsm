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

use std::collections::HashMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize};

use anyhow::{Ok, Result, anyhow};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;

fn table_out_of_range(
    lower: &Bound<&[u8]>,
    upper: &Bound<&[u8]>,
    table_lower: &[u8],
    table_upper: &[u8],
) -> bool {
    let before_range = match upper {
        Bound::Included(k) => *k < table_lower,
        Bound::Excluded(k) => *k <= table_lower,
        Bound::Unbounded => false,
    };

    if before_range {
        return true;
    }

    match lower {
        Bound::Included(k) => *k > table_upper,
        Bound::Excluded(k) => *k >= table_upper,
        Bound::Unbounded => false,
    }
}

use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::{MemTable, MemTableIterator};
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

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
    pub(crate) is_in_compact: AtomicBool,
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
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();

        if let Some(handle) = self.flush_thread.lock().take() {
            handle
                .join()
                .map_err(|e| anyhow!("flush thread panicked: {:?}", e))?;
        }

        if let Some(handle) = self.compaction_thread.lock().take() {
            handle
                .join()
                .map_err(|e| anyhow!("compaction thread panicked: {:?}", e))?;
        }

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

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let state = LsmStorageState::create(&options);

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

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
            is_in_compact: AtomicBool::new(false),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let key_slice = KeySlice::from_slice(key);

        // Check memtable first
        match self.state.read().memtable.get(key) {
            Some(v) => {
                if v == Bytes::new() {
                    Ok(None)
                } else {
                    Ok(Some(v))
                }
            }
            None => {
                // Check immutable memtables
                for imm_memtable in &self.state.read().imm_memtables {
                    if let Some(v) = imm_memtable.get(key) {
                        if v == Bytes::new() {
                            return Ok(None);
                        } else {
                            return Ok(Some(v));
                        }
                    }
                }

                // Snapshot the state to check SSTables outside the lock
                let snapshot = self.state.read().clone();
                let l0_sstables = snapshot.l0_sstables.clone();
                let levels = snapshot.levels.clone();
                let sstables = snapshot.sstables.clone();
                // Lock is now released

                // Check SSTables (latest to oldest)
                // Check L0 SSTables
                if self.compaction_controller.flush_to_l0() {
                    for sst_id in l0_sstables {
                        if let Some(sst) = sstables.get(&sst_id)
                            && key_slice >= sst.first_key().as_key_slice()
                            && key_slice <= sst.last_key().as_key_slice()
                            && sst
                                .bloom
                                .as_ref()
                                .unwrap()
                                .may_contain(farmhash::fingerprint32(key))
                            && let Some((_k, v)) = sst.get(key_slice)?
                        {
                            if !v.is_empty() {
                                return Ok(Some(v));
                            } else {
                                return Ok(None); // Tombstone
                            }
                        }
                    }
                }

                // Check all levels from L1 to max_level
                for (_level, level_sst_ids) in levels {
                    for sst_id in level_sst_ids {
                        if let Some(sst) = sstables.get(&sst_id)
                            && key_slice >= sst.first_key().as_key_slice()
                            && key_slice <= sst.last_key().as_key_slice()
                            && sst
                                .bloom
                                .as_ref()
                                .unwrap()
                                .may_contain(farmhash::fingerprint32(key))
                            && let Some((_k, v)) = sst.get(key_slice)?
                        {
                            if !v.is_empty() {
                                return Ok(Some(v));
                            } else {
                                return Ok(None); // Tombstone
                            }
                        }
                    }
                }

                Ok(None)
            }
        }
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if (self.state.read().memtable.approximate_size() + key.len() + value.len())
            >= self.options.target_sst_size
        {
            self.force_freeze_memtable(&self.state_lock.lock())?;
        }
        self.state.write().memtable.put(key, value)
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.put(key, &[])
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
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let curr_state = self.state.write().clone();
        let new_state = Arc::new(LsmStorageState {
            memtable: Arc::new(MemTable::create(self.next_sst_id())),
            imm_memtables: {
                let mut imm_memtables = curr_state.imm_memtables.clone();
                imm_memtables.insert(0, curr_state.memtable.clone());
                imm_memtables
            },
            l0_sstables: curr_state.l0_sstables.clone(),
            levels: curr_state.levels.clone(),
            sstables: curr_state.sstables.clone(),
        });
        *(self.state.write()) = new_state;
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        // 外层循环用于重试整个过程，直到成功更新状态或者确定无需刷新
        loop {
            let memtable_to_flush_id; // 只需要ID来标识，实际的MemTable可能需要重新获取
            let sst_id;
            let old_state_arc; // RwLock中当前最新的Arc<LsmStorageState>

            // 1. 获取当前状态的快照，并决定要刷新的MemTable
            {
                let state_read_guard = self.state.read();
                old_state_arc = state_read_guard.clone(); // 克隆内部的Arc<LsmStorageState>

                // 如果 imm_memtables 为空，则没有可刷新的，直接返回
                if old_state_arc.imm_memtables.is_empty() {
                    return Ok(());
                }

                memtable_to_flush_id = old_state_arc
                    .imm_memtables
                    .last()
                    .expect("imm_memtables should not be empty here, checked above.")
                    .id(); // 只需要ID，MemTable对象本身可能不需要在锁外传递

                // 确保 next_sst_id 是线程安全的（例如，使用 AtomicUsize）
                sst_id = self.next_sst_id();
            } // 读锁在这里释放

            // 2. 锁外进行耗时操作 (刷盘)
            // 从 old_state_arc 中获取 memtable_to_flush_id 对应的 MemTable 实例
            // 这里需要小心：如果 memtable_to_flush_id 对应的 MemTable 已经被移除，那么这里可能会失败。
            // 最安全的做法是，在最初获取到 memtable_to_flush 的时候，就克隆其内容，而不是克隆 Arc，
            // 这样即使其原始 Arc 被替换，你手里的内容依然有效。
            let memtable_to_flush_instance = old_state_arc
                .imm_memtables
                .iter()
                .find(|m| m.id() == memtable_to_flush_id)
                .cloned() // 克隆 ImmutableMemtable 的内容
                .expect("Memtable to flush not found in snapshot, should not happen after check.");

            let mut builder = SsTableBuilder::new(self.options.block_size);
            memtable_to_flush_instance.flush(&mut builder)?; // 使用克隆的内容进行刷盘
            let path = self.path_of_sst(sst_id);
            let sstable = builder.build(sst_id, Some(self.block_cache.clone()), path)?;

            // 3. 进入临界区，获取写锁并尝试更新状态
            let mut state_write_guard = self.state.write();

            // 关键步骤：在持有写锁时，重新检查并基于最新的状态进行操作
            let latest_state_arc_in_lock = state_write_guard.clone(); // 获取RwLock中当前最新的Arc<LsmStorageState>

            // a) 检查 imm_memtables 是否仍然为空，或者要刷新的 MemTable 是否依然是最后一个。
            if latest_state_arc_in_lock.imm_memtables.is_empty() {
                // 在耗时操作期间，imm_memtables 被清空了。
                // 本次 flush 任务已经不需要执行，直接成功返回。
                return Ok(());
            }

            // 检查 `memtable_to_flush_id` 是否仍然是 `imm_memtables` 列表中的最后一个。
            // 如果不是，说明在耗时操作期间，列表被修改了（可能是另一个 flush 完成了，
            // 或者有新的 imm_memtable 被添加到末尾）。
            // 这种情况下，本次 flush 尝试是无效的，需要放弃并重试整个循环。
            if latest_state_arc_in_lock
                .imm_memtables
                .last()
                .map(|m| m.id())
                != Some(memtable_to_flush_id)
            {
                // 状态已变更，放弃本次更新，回到循环开头重试
                continue;
            }
            // 如果代码执行到这里，说明 memtable_to_flush_id 仍然是 imm_memtables 列表的最后一个元素，
            // 并且列表不为空。我们可以安全地基于 latest_state_arc_in_lock 进行修改。

            // b) 基于最新状态 latest_state_arc_in_lock 构建新的状态
            let mut imm_memtables = latest_state_arc_in_lock.imm_memtables.clone();
            imm_memtables.pop(); // 移除最新的 imm_memtable
            let mut l0_sstables = latest_state_arc_in_lock.l0_sstables.clone();
            let mut levels = latest_state_arc_in_lock.levels.clone();
            let mut sstables = latest_state_arc_in_lock.sstables.clone();
            sstables.insert(sst_id, Arc::new(sstable));

            if self.compaction_controller.flush_to_l0() {
                l0_sstables.insert(0, sst_id);
            } else {
                // Tiered compaction: flush to new tier
                levels.insert(0, (sst_id, vec![sst_id]));
            }

            let new_state_arc = Arc::new(LsmStorageState {
                memtable: latest_state_arc_in_lock.memtable.clone(),
                imm_memtables,
                l0_sstables,
                levels,
                sstables,
            });

            // c) 原子替换 RwLock 内部的 Arc<LsmStorageState>
            *state_write_guard = new_state_arc;

            // 成功更新，退出循环
            return Ok(());
        }
    }
    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        // ---- Step 1: Memtables ----
        let lock = self.state.read();
        let mut mem_children: Vec<Box<MemTableIterator>> = Vec::new();

        mem_children.push(Box::new(lock.memtable.scan(lower, upper)));
        mem_children.extend(
            lock.imm_memtables
                .iter()
                .map(|m| Box::new(m.scan(lower, upper))),
        );
        let memtable_iter = MergeIterator::create(mem_children);

        // ---- Step 2: Clone SST Info ----
        let l0_sstables = lock.l0_sstables.clone();
        let levels = lock.levels.clone();
        let sstables = lock.sstables.clone();
        drop(lock);

        // ---- Step 3: SST Iterators ----
        let mut sst_children: Vec<Box<SsTableIterator>> = Vec::new();
        let lower_key_slice = match &lower {
            Bound::Included(k) | Bound::Excluded(k) => KeySlice::from_slice(k),
            Bound::Unbounded => KeySlice::from_slice(b""),
        };

        // Handle L0 SSTables
        if self.compaction_controller.flush_to_l0() {
            for sst_id in l0_sstables {
                if let Some(sst) = sstables.get(&sst_id) {
                    let table_lower = sst.first_key().raw_ref();
                    let table_upper = sst.last_key().raw_ref();

                    if table_out_of_range(&lower, &upper, table_lower, table_upper) {
                        continue;
                    }

                    let mut it =
                        SsTableIterator::create_and_seek_to_key(sst.clone(), lower_key_slice)?;

                    if let Bound::Excluded(excl) = &lower
                        && it.is_valid()
                        && it.key() == KeySlice::from_slice(excl)
                    {
                        it.next()?;
                    }

                    if it.is_valid() {
                        sst_children.push(Box::new(it));
                    }
                }
            }
        }

        // Create iterators for all levels
        let mut level_iters: Vec<SstConcatIterator> = Vec::new();
        for (_level, level_sst_ids) in levels {
            if level_sst_ids.is_empty() {
                continue;
            }

            // Create ordered SSTs for this level
            let mut ordered_ssts = Vec::new();
            for sst_id in level_sst_ids {
                if let Some(sst) = sstables.get(&sst_id) {
                    ordered_ssts.push(sst.clone());
                }
            }

            if ordered_ssts.is_empty() {
                continue;
            }

            // Create SstConcatIterator for this level
            let level_iter = match &lower {
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(ordered_ssts)?,
                Bound::Excluded(low) | Bound::Included(low) => {
                    SstConcatIterator::create_and_seek_to_key(
                        ordered_ssts,
                        KeySlice::from_slice(low),
                    )?
                }
            };

            level_iters.push(level_iter);
        }

        // ---- Step 4: Merge and return ----
        let sst_iter = MergeIterator::create(sst_children);

        // Merge all level iterators
        let level_merge = MergeIterator::create(level_iters.into_iter().map(Box::new).collect());

        // Merge memtable + L0 with all levels
        let mem_l0 = TwoMergeIterator::create(memtable_iter, sst_iter)?;
        let final_iter = TwoMergeIterator::create(mem_l0, level_merge)?;

        let iter = LsmIterator::new(final_iter, upper.map(Bytes::copy_from_slice))?;
        Ok(FusedIterator::new(iter))
    }
}
