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
use crate::key::{Key, KeySlice};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
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

        // Flush all memtables if WAL is disabled
        if !self.inner.options.enable_wal {
            // Freeze current memtable if not empty
            if !self.inner.state.read().memtable.is_empty() {
                self.inner
                    .force_freeze_memtable(&self.inner.state_lock.lock())?;
            }
            // Flush all imm_memtables
            while !self.inner.state.read().imm_memtables.is_empty() {
                self.inner.force_flush_next_imm_memtable()?;
            }
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
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let manifest_path = path.join("MANIFEST");
        // 1. 恢复 Manifest
        let (manifest, records) = Manifest::recover(&manifest_path)?;

        // 创建初始状态 (默认包含一个 ID=0 的空 MemTable)
        let mut state = LsmStorageState::create(&options);

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

        // 2. 回放 Manifest 记录，重建 MemTable 的结构
        let mut max_sst_id = 0;
        for record in records {
            match record {
                ManifestRecord::Flush(sst_id) => {
                    max_sst_id = max_sst_id.max(sst_id);
                    // 只有当 Flush 时，才将 memtable 从内存中移除（变成 L0 SST）
                    if compaction_controller.flush_to_l0() {
                        state.l0_sstables.insert(0, sst_id);
                    } else {
                        state.levels.insert(0, (sst_id, vec![sst_id]));
                    }
                    // 注意：实际的 memtable 移除逻辑通常配合 NewMemtable 发生，
                    // 或者在这里如果需要清理 imm_memtables 中对应的 ID。
                    // 简化的 Mini-LSM 中，Flush 记录通常意味着对应的 ImmMemTable 已经落盘，
                    // 但 Manifest Replay 主要是为了构建 SST 列表。
                    // 准确的 ImmMemTable 列表主要依赖 NewMemtable 记录来推导。
                }
                ManifestRecord::NewMemtable(id) => {
                    max_sst_id = max_sst_id.max(id);
                    // [修改点 A] 遇到新 MemTable 记录，说明前一个 MemTable 变成了 Immutable
                    // 只有当 ID > 0 (即不是初始状态) 时才移动，防止把初始空表放进去
                    if state.memtable.id() > 0 {
                        state.imm_memtables.insert(0, state.memtable.clone());
                    }
                    // 创建一个新的空 MemTable 占位，稍后用 WAL 填充
                    state.memtable = Arc::new(MemTable::create(id));
                }
                ManifestRecord::Compaction(task, output) => {
                    for &id in &output {
                        max_sst_id = max_sst_id.max(id);
                    }
                    let (new_state, _) =
                        compaction_controller.apply_compaction_result(&state, &task, &output, true);
                    state = new_state;
                }
            }
        }

        println!("Final State to Load:");
        println!("L0 SSTs: {:?}", state.l0_sstables);
        for (level_idx, level_ssts) in &state.levels {
            println!("Level {}: {:?}", level_idx, level_ssts);
        }

        // 3. 加载 SSTs (保持原样)
        let mut sstables = HashMap::new();
        let all_sst_ids = state
            .l0_sstables
            .iter()
            .chain(state.levels.iter().flat_map(|(_, ids)| ids.iter()));
        for &id in all_sst_ids {
            let sst_path = Self::path_of_sst_static(path, id);
            let file = crate::table::FileObject::open(&sst_path)?;
            let sst = SsTable::open(id, Some(Arc::new(BlockCache::new(1024))), file)?;
            sstables.insert(id, Arc::new(sst));
        }
        state.sstables = sstables;

        let next_sst_id = AtomicUsize::new(max_sst_id + 1);

        if options.enable_wal {
            // 我们需要从磁盘读取 WAL 文件，恢复 MemTable 的内容

            // 4.1 恢复 Immutable MemTables
            let mut new_imm_memtables = Vec::new();
            for mem in state.imm_memtables {
                let id = mem.id();
                let wal_path = path.join(format!("{:05}.wal", id));
                // 重建 MemTable 对象 (带 WAL)
                new_imm_memtables.push(Arc::new(MemTable::recover_from_wal(id, wal_path)?));
            }
            state.imm_memtables = new_imm_memtables;

            // 4.2 恢复当前的 Mutable MemTable
            // Manifest Replay 结束时，state.memtable 指向的是最后一次 NewMemtable 的 ID
            // 如果 Manifest 为空（全新数据库），这里的 ID 可能是 0
            if state.memtable.id() > 0 {
                let id = state.memtable.id();
                let wal_path = path.join(format!("{:05}.wal", id));
                state.memtable = Arc::new(MemTable::recover_from_wal(id, wal_path)?);
                // 更新 next_id，基于当前恢复的 MemTable ID
                next_sst_id.store(id + 1, std::sync::atomic::Ordering::SeqCst);
            } else {
                // 如果是全新的库 (ID=0)，我们需要创建一个 ID=1 的新表
                let next_id = max_sst_id + 1;
                let wal_path = path.join(format!("{:05}.wal", next_id));
                state.memtable = Arc::new(MemTable::create_with_wal(next_id, wal_path)?);
                // 记得记录到 Manifest，否则下次启动不知道有这个表
                manifest.add_record_when_init(ManifestRecord::NewMemtable(next_id))?;
                next_sst_id.store(next_id + 1, std::sync::atomic::Ordering::SeqCst);
            }
        } else {
            // == WAL 关闭模式 (原有逻辑) ==
            // 不从 WAL 恢复，直接创建一个全新的 MemTable
            // 之前内存里的旧数据因为没有 WAL 都丢失了，所以直接清空
            state.imm_memtables.clear();

            let next_id = max_sst_id + 1;
            state.memtable = Arc::new(MemTable::create(next_id));
            next_sst_id.store(next_id + 1, std::sync::atomic::Ordering::SeqCst);
        }

        // 5. 排序 Levels (保持原样)
        if matches!(
            compaction_controller,
            CompactionController::Leveled(_) | CompactionController::Simple(_)
        ) {
            for (_, level_ssts) in &mut state.levels {
                level_ssts
                    .sort_by_key(|&id| state.sstables.get(&id).unwrap().first_key().as_key_slice());
            }
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id,
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
            is_in_compact: AtomicBool::new(false),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        let state = self.state.read();

        if self.options.enable_wal {
            state.memtable.sync_wal()?;
        }

        Ok(())
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
                    if level_sst_ids.is_empty() {
                        continue;
                    }
                    // Binary search to find the SST that may contain the key
                    let pos = level_sst_ids.partition_point(|&id| {
                        sstables
                            .get(&id)
                            .map(|sst| sst.first_key().as_key_slice() <= key_slice)
                            .unwrap_or(false)
                    });
                    if pos > 0 {
                        let sst_id = level_sst_ids[pos - 1];
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
        std::fs::File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        // 1. 获取下一个 MemTable/SST 的 ID
        let memtable_id = self.next_sst_id();

        // 2. 根据是否启用 WAL 创建新的 MemTable
        let memtable = if self.options.enable_wal {
            // 构造 WAL 路径，通常是 `<storage_dir>/<id>.wal`
            // 假设你已经在 lsm_storage.rs 中实现了 path_of_wal 辅助函数
            let path = self.path_of_wal(memtable_id);
            Arc::new(MemTable::create_with_wal(memtable_id, path)?)
        } else {
            Arc::new(MemTable::create(memtable_id))
        };

        // 3. 核心修改：如果开启了 WAL，必须在切换内存状态前将 "NewMemtable" 记录写入 Manifest。
        // 这样如果系统随后立刻崩溃，重启时的 recover 过程才知道去加载这个 memtable_id 对应的 WAL。
        if self.options.enable_wal
            && let Some(manifest) = &self.manifest
        {
            manifest.add_record(
                state_lock_observer,
                ManifestRecord::NewMemtable(memtable_id),
            )?;
        }

        // 4. 更新内存状态 (State Update)
        // 使用 RwLock 的写锁来更新全局状态
        let mut guard = self.state.write();
        let mut snapshot = guard.as_ref().clone();

        // 将旧的 memtable 移动到 immutable 列表头部
        let old_memtable = std::mem::replace(&mut snapshot.memtable, memtable);
        snapshot.imm_memtables.insert(0, old_memtable);

        // 更新状态指针
        *guard = Arc::new(snapshot);

        Ok(())
    }

    pub fn verify_lsm_invariants(&self, state: &LsmStorageState) {
        // 1. 检查 L0: 允许重叠，无需检查 Key 范围
        // 但可以检查文件是否存在
        println!("Checking L0: {:?}", state.l0_sstables);
        for &sst_id in &state.l0_sstables {
            if !state.sstables.contains_key(&sst_id) {
                panic!("L0 SST {} missing in sstables map", sst_id);
            }
        }

        // 2. 检查 L1+: 不允许 Key 重叠，且必须有序
        for (level_idx, (_, sst_ids)) in state.levels.iter().enumerate() {
            let mut prev_end_key: Option<&[u8]> = None;

            for &sst_id in sst_ids {
                let sst = state.sstables.get(&sst_id).expect("Level SST missing");
                let first = sst.first_key().as_key_slice();
                let last = sst.last_key().as_key_slice();

                // 检查：当前 SST 的开始 Key 必须 > 上一个 SST 的结束 Key
                if let Some(prev_end) = prev_end_key
                    && first <= Key::from_slice(prev_end)
                {
                    panic!(
                        "LSM Invariant Violated at Level {}: SST {} [{:?}] overlaps/disordered with previous SST end [{:?}]",
                        level_idx + 1,
                        sst_id,
                        first,
                        prev_end
                    );
                }

                prev_end_key = Some(last.raw_ref());
            }
        }
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        // 1. 获取快照并决定要刷新的 MemTable
        // 在 insert(0) 模式下，Vec 的最后一个元素（last）是最老的
        let (memtable_to_flush, sst_id) = {
            let guard = self.state.read();

            if guard.imm_memtables.is_empty() {
                return Ok(());
            }

            // 获取最后一个元素（最老的）
            let memtable = guard
                .imm_memtables
                .last()
                .expect("imm_memtables should not be empty")
                .clone();

            let sst_id = self.next_sst_id();

            (memtable, sst_id)
        }; // 读锁释放

        // 2. 锁外耗时操作：刷盘
        let mut builder = SsTableBuilder::new(self.options.block_size);
        memtable_to_flush.flush(&mut builder)?;
        let path = self.path_of_sst(sst_id);
        let sstable = builder.build(sst_id, Some(self.block_cache.clone()), path)?;

        // 3. 获取写锁，更新状态
        let mut guard = self.state.write();

        let mut latest_state = guard.as_ref().clone();

        // 再次检查：我们刚才刷盘的那个 memtable，是否仍然是 imm_memtables 的最后一个？
        let current_last_id = latest_state.imm_memtables.last().map(|m| m.id());

        if current_last_id != Some(memtable_to_flush.id()) {
            // 如果 ID 变了，说明已经被其他线程刷走了，直接返回
            return Ok(());
        }

        // 1. 从内存表中移除最老的（尾部）
        latest_state.imm_memtables.pop();

        // 2. 将新生成的 SST 加入架构
        latest_state.sstables.insert(sst_id, Arc::new(sstable));

        // 3. 更新 L0 或 Levels
        if self.compaction_controller.flush_to_l0() {
            latest_state.l0_sstables.insert(0, sst_id);
        } else {
            // Tiered compaction 逻辑
            latest_state.levels.insert(0, (sst_id, vec![sst_id]));
        }

        // 4. 更新 Manifest
        self.sync_dir()?;
        if let Some(manifest) = &self.manifest {
            manifest.add_record_when_init(crate::manifest::ManifestRecord::Flush(sst_id))?;
        }

        // 5. 原子替换状态
        // 将修改后的结构体包装成新的 Arc，替换掉锁里面的旧 Arc
        *guard = Arc::new(latest_state);

        Ok(())
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
