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

use std::ops::Bound;

use anyhow::{Result, anyhow};
use bytes::Bytes;

use crate::{
    iterators::{
        StorageIterator, concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator,
    },
    mem_table::MemTableIterator,
    table::iterator::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the course for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
    read_ts: u64,
    is_valid: bool,
    last_returned_key: Option<Vec<u8>>,
}

impl LsmIterator {
    pub(crate) fn new(
        iter: LsmIteratorInner,
        end_bound: Bound<Bytes>,
        read_ts: u64,
    ) -> Result<Self> {
        let mut it = Self {
            is_valid: iter.is_valid(),
            inner: iter,
            end_bound,
            read_ts,
            last_returned_key: None, // 初始必须为 None
        };

        // 1. 初始空校验
        if !it.is_valid() {
            return Ok(it);
        }

        // 2. 初始移动逻辑 (核心修复)
        // 我们需要找到第一个“可见”且“非 Tombstone”的 Key
        it.move_to_first_valid()?;

        Ok(it)
    }

    fn check_end_bound(&self) -> bool {
        match &self.end_bound {
            Bound::Unbounded => true,
            Bound::Included(key) => self.key() <= key.as_ref(),
            Bound::Excluded(key) => self.key() < key.as_ref(),
        }
    }

    fn move_to_first_valid(&mut self) -> Result<()> {
        loop {
            // 1. 基础有效性检查
            if !self.inner.is_valid() {
                self.is_valid = false;
                return Ok(());
            }
            if !self.check_end_bound() {
                self.is_valid = false;
                return Ok(());
            }

            // 2. 检查时间戳：如果当前版本太新，直接跳过，不要更新 last_returned_key
            if self.inner.key().ts() > self.read_ts {
                self.inner.next()?;
                continue;
            }

            // 3. 此时 ts <= read_ts，说明我们找到了该 Key 在快照下的最新版本
            // 此时才记录 Key，为了屏蔽后续更旧的版本
            self.last_returned_key = Some(self.inner.key().key_ref().to_vec());

            // 4. 检查是否为 Tombstone
            if self.inner.value().is_empty() {
                // 如果是删除标记，说明该 Key 被删除了。
                // 我们调用 self.next()。注意：这里调用 next() 是安全的，
                // 因为 last_returned_key 已经设置了，next() 会自动跳过该 Key 的剩余旧版本，
                // 直接去找下一个不同的 Key。
                self.next()?;
                continue;
            }

            // 找到有效数据，退出循环
            return Ok(());
        }
    }
}

impl StorageIterator for LsmIterator {
    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.is_valid
            && match &self.end_bound {
                Bound::Unbounded => true,
                Bound::Included(key) => self.key() <= key.as_ref(),
                Bound::Excluded(key) => self.key() < key.as_ref(),
            }
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        loop {
            // 1. 推进到底层迭代器
            self.inner.next()?;

            // 2. 检查有效性
            if !self.inner.is_valid() {
                self.is_valid = false;
                return Ok(());
            }

            // 3. 检查边界
            if !self.check_end_bound() {
                self.is_valid = false;
                return Ok(());
            }

            // 4. 获取当前 User Key
            let current_key_ref = self.inner.key().key_ref();

            // 5. 判断是否是新 Key
            // 如果 last_returned_key 和当前 key 一样，说明这是同一个 key 的更老版本
            // 我们直接跳过（因为我们已经处理过该 key 的最新可见版本了）
            if let Some(last) = &self.last_returned_key
                && last == current_key_ref
            {
                continue;
            }

            // 走到这里，说明遇到了一个全新的 Key（或者之前遇到的版本都因为太新被跳过了）

            // [关键步骤 A] 检查时间戳是否在读取范围
            // 如果当前版本太新，我们不能算作“看见”了这个 Key。
            // 我们不更新 last_returned_key，直接 continue。
            // 这样下一次循环遇到该 Key 的旧版本时，上面的 if 判断依然不成立，
            // 从而有机会进入这里的逻辑。
            if self.inner.key().ts() > self.read_ts {
                continue;
            }

            // [关键步骤 B] 既然时间戳符合要求，这个 Key 版本就是当前快照下的最新版本。
            // 现在记录这个 Key，屏蔽掉后续更老的版本。
            self.last_returned_key = Some(current_key_ref.to_vec());

            // [关键步骤 C] 检查 Value 是否为空 (Tombstone)
            // 虽然是 Tombstone，但我们已经在步骤 B 记录了 Key，
            // 所以后续的老版本依然会被最上面的 if 过滤掉。这是正确的。
            if self.inner.value().is_empty() {
                continue;
            }

            // 只有非空的有效值，且时间戳在范围内，才返回给用户
            return Ok(());
        }
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    fn num_active_iterators(&self) -> usize {
        if self.has_errored {
            0
        } else {
            self.iter.num_active_iterators()
        }
    }
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            return Err(anyhow!("error"));
        }

        if !self.iter.is_valid() {
            return Ok(()); // 空操作
        }
        match self.iter.next() {
            Ok(()) => Ok(()),
            Err(_) => {
                // 出错后标记无效，以后就是空
                self.has_errored = true;
                Err(anyhow!("error"))
            }
        }
    }
}
