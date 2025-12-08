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
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{StorageIterator, two_merge_iterator::TwoMergeIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
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
        println!("read ts {}", self.read_ts);
        // First probe local storage
        if let Some(entry) = self.local_storage.get(key) {
            let value = entry.value().clone();
            // If value is empty, it's a deletion marker - return None
            // Otherwise, return the value
            return Ok(if value.is_empty() { None } else { Some(value) });
        }
        // Not found in local storage, check LSM
        self.inner.get_with_ts(key, self.read_ts)
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
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        if self.committed.load(Ordering::SeqCst) {
            return Err(anyhow::anyhow!("Transaction already committed"));
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::new());
        Ok(())
    }

    pub fn commit(&self) -> Result<()> {
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
        self.inner.write_batch(&batch)?;
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
    _txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        mut iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        // Initialize by moving to first valid position
        loop {
            if !iter.is_valid() {
                return Ok(Self { _txn: txn, iter });
            }
            if iter.value().is_empty() {
                iter.next()?;
                continue;
            }
            return Ok(Self { _txn: txn, iter });
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
        loop {
            self.iter.next()?;
            if !self.iter.is_valid() {
                return Ok(());
            }
            // Skip deletion markers (empty values)
            if self.iter.value().is_empty() {
                continue;
            }
            return Ok(());
        }
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
