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

use std::{
    collections::HashSet,
    ops::Bound,
    sync::{Arc, atomic::AtomicBool, atomic::Ordering},
};

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use farmhash;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{StorageIterator, two_merge_iterator::TwoMergeIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
    mvcc::CommittedTxnData,
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.committed.load(Ordering::SeqCst) {
            return Err(anyhow::anyhow!("Transaction already committed"));
        }
        let key_hash = if self.key_hashes.is_some() {
            farmhash::hash32(key)
        } else {
            0
        };
        println!("read ts {}", self.read_ts);
        // First probe local storage
        if let Some(entry) = self.local_storage.get(key) {
            let value = entry.value().clone();
            // If value is empty, it's a deletion marker - return None
            // Otherwise, return the value
            let result = Ok(if value.is_empty() { None } else { Some(value) });
            if self.key_hashes.is_some() {
                self.key_hashes.as_ref().unwrap().lock().0.insert(key_hash);
            }
            return result;
        }
        // Not found in local storage, check LSM
        let result = self.inner.get_with_ts(key, self.read_ts);
        if self.key_hashes.is_some() {
            self.key_hashes.as_ref().unwrap().lock().0.insert(key_hash);
        }
        result
    }

    fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
        match bound {
            Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
            Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    // 在 Transaction impl 中
    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        if self.committed.load(Ordering::SeqCst) {
            return Err(anyhow::anyhow!("Transaction already committed"));
        }
        // 1. 创建 LSM 迭代器
        let lsm_iter = self.inner.scan_with_ts(lower, upper, self.read_ts)?;

        // 2. 准备本地迭代器的边界 (需要 clone 因为闭包要捕获)
        let local_lower = Self::map_bound(lower);
        let local_upper = Self::map_bound(upper);

        // 3. 创建本地迭代器
        let mut local_iter = TxnLocalIterator::try_new(
            self.local_storage.clone(),
            move |map: &Arc<SkipMap<Bytes, Bytes>>| {
                // 注意 move
                Ok::<_, anyhow::Error>(map.range((
                    local_lower.clone(), // 使用转换后的边界
                    local_upper.clone(),
                )))
            },
            (Bytes::new(), Bytes::new()),
        )?;

        local_iter.next()?;

        // 4. 合并
        let merge_iter = TwoMergeIterator::create(local_iter, lsm_iter)?;
        TxnIterator::create(self.clone(), merge_iter)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.committed.load(Ordering::SeqCst) {
            return Err(anyhow::anyhow!("Transaction already committed"));
        }
        if self.key_hashes.is_some() {
            let key_hash = farmhash::hash32(key);
            self.key_hashes.as_ref().unwrap().lock().1.insert(key_hash);
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        if self.committed.load(Ordering::SeqCst) {
            return Err(anyhow::anyhow!("Transaction already committed"));
        }
        if self.key_hashes.is_some() {
            let key_hash = farmhash::hash32(key);
            self.key_hashes.as_ref().unwrap().lock().1.insert(key_hash);
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::new());
        Ok(())
    }

    pub fn commit(&self) -> Result<()> {
        // 0. 防止重复提交
        if self.committed.load(Ordering::SeqCst) {
            return Ok(());
        }

        // 检查本地是否有写入数据
        // 如果本地完全没有数据修改，甚至不需要获取 commit_lock，直接返回即可
        // (这一步是性能优化，非必须，但推荐)
        if self.local_storage.is_empty() {
            self.committed.store(true, Ordering::SeqCst);
            return Ok(());
        }

        // 1. 获取 MVCC 全局提交锁
        let mvcc = self.inner.mvcc.as_ref().unwrap();
        let _commit_guard = mvcc.commit_lock.lock();

        // 2. 验证阶段 (只针对 Serializable 模式)
        // 我们需要一个变量来决定是否需要在提交后更新 committed_txns
        let serializable_write_set = if let Some(guard) = &self.key_hashes {
            let guard = guard.lock();
            let (read_set, write_set) = (&guard.0, &guard.1);

            // 2.1 只读事务优化：如果开启了串行化但没有写操作，直接成功
            if write_set.is_empty() {
                self.committed.store(true, Ordering::SeqCst);
                return Ok(());
            }

            // 2.2 冲突检测
            // 只有 serializable 模式才需要去查 committed_txns
            let committed_txns = mvcc.committed_txns.lock();

            // 遍历 (read_ts, +inf) 范围内的所有已提交事务
            for (_, txn_data) in
                committed_txns.range((Bound::Excluded(self.read_ts), Bound::Unbounded))
            {
                // 检查：我的读集 ∩ 别人的写集
                for hash in read_set {
                    if txn_data.key_hashes.contains(hash) {
                        return Err(anyhow::anyhow!("Validation failed: serializable conflict"));
                    }
                }
            }

            // 验证通过，返回 write_set 以便稍后更新历史记录
            Some(write_set.clone())
        } else {
            // 【关键修改】
            // 如果没有开启 serializable (key_hashes 为 None)
            // 我们不做验证，也不需要返回 write_set 给 committed_txns
            // 但我们 绝对不能 在这里 return Ok(())，必须让它继续往下走去执行写入！
            None
        };

        // 3. 写入阶段：构造 WriteBatch
        // 无论是否 serializable，只要代码走到这里，说明都允许写入
        let batch: Vec<WriteBatchRecord<Bytes>> = self
            .local_storage
            .iter()
            .map(|entry| {
                let key = entry.key();
                let value = entry.value();
                if value.is_empty() {
                    WriteBatchRecord::Del(key.clone())
                } else {
                    WriteBatchRecord::Put(key.clone(), value.clone())
                }
            })
            .collect();

        // 4. 执行写入并获取提交时间戳 (Commit TS)
        // 这一步会分配 commit_ts 并更新 MVCC 水位
        let commit_ts = self.inner.write_batch_inner(&batch)?;

        // 5. 记录阶段：更新已提交事务历史 (仅 Serializable 模式需要)
        if let Some(write_set) = serializable_write_set {
            let mut committed_txns = mvcc.committed_txns.lock();
            committed_txns.insert(
                commit_ts,
                CommittedTxnData {
                    key_hashes: write_set,
                    read_ts: self.read_ts,
                    commit_ts,
                },
            );

            // 可选：清理太久远的历史记录以节省内存（Watermark 机制）
            // 这一步通常在 Compaction 或独立线程做，但在这里顺手做也是可以的
        }

        // 6. 标记事务完成
        self.committed.store(true, Ordering::SeqCst);

        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        let mut ts_guard = self.inner.mvcc.as_ref().unwrap().ts.lock();

        ts_guard.1.remove_reader(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.with_item(|item| item.1.as_ref())
    }

    fn key(&self) -> &[u8] {
        self.with_item(|item| item.0.as_ref())
    }

    fn is_valid(&self) -> bool {
        !self.with_item(|item| item.0.is_empty())
    }

    fn next(&mut self) -> Result<()> {
        let mut next_item: Option<(Bytes, Bytes)> = None;

        self.with_iter_mut(|iter| {
            if let Some(entry) = iter.next() {
                next_item = Some((entry.key().clone(), entry.value().clone()));
            } else {
                next_item = None;
            }
        });

        match next_item {
            Some((k, v)) => {
                self.with_item_mut(|item| {
                    *item = (k, v);
                });
            }
            None => {
                self.with_item_mut(|item| {
                    *item = (Bytes::new(), Bytes::new());
                });
            }
        }

        Ok(())
    }
}

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        // Initialize by moving to first valid position
        let mut txn_iter = Self { txn, iter };

        // 跳过删除标记
        txn_iter.skip_deletes()?;

        // 如果初始位置有效，记录到读取集
        txn_iter.add_to_read_set_if_valid();

        Ok(txn_iter)
    }

    fn skip_deletes(&mut self) -> Result<()> {
        // 只要当前有效，且 Value 为空（Tombstone），就继续往后找
        while self.iter.is_valid() && self.iter.value().is_empty() {
            self.iter.next()?;
        }
        Ok(())
    }

    fn add_to_read_set_if_valid(&self) {
        // 1. 检查是否开启了串行化 (Serializability)
        // key_hashes 只有在 serializable 模式下才是 Some
        if let Some(guard) = &self.txn.key_hashes {
            if self.iter.is_valid() {
                // 2. 计算哈希
                let key = self.iter.key();
                let hash = farmhash::fingerprint32(key);

                // 3. 上锁并记录
                let mut guard = guard.lock();
                guard.0.insert(hash); // guard.0 是 read_set
            }
        }
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid() && !self.iter.value().is_empty()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        self.skip_deletes()?;

        self.add_to_read_set_if_valid();

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
