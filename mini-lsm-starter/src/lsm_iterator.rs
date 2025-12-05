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
    is_valid: bool,
    last_returned_key: Option<Vec<u8>>,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, end_bound: Bound<Bytes>) -> Result<Self> {
        let mut it = Self {
            is_valid: iter.is_valid(),
            inner: iter,
            end_bound,
            last_returned_key: None,
        };

        // 1. 如果迭代器一开始就无效，直接返回
        if !it.is_valid() {
            return Ok(it);
        }

        // 2. 检查是否超出用户设定的上界
        if !it.check_end_bound() {
            it.is_valid = false;
            return Ok(it);
        }

        // 3. 初始化 last_returned_key
        // 这一步是为了让后续的 next() 逻辑能够识别“当前 Key 已经被处理过”，
        // 从而正确跳过该 Key 的旧版本。
        let current_key_ref = it.inner.key().key_ref();
        it.last_returned_key = Some(current_key_ref.to_vec());

        // 4.检查初始位置是否是 Tombstone
        // 如果当前位置是删除标记（Value 为空），我们需要调用 next() 跳过它及其旧版本，
        // 直到找到第一个有效的非删除 Key。
        if it.inner.value().is_empty() {
            it.next()?;
        }

        Ok(it)
    }

    fn check_end_bound(&self) -> bool {
        match &self.end_bound {
            Bound::Unbounded => true,
            Bound::Included(key) => self.key() <= key.as_ref(),
            Bound::Excluded(key) => self.key() < key.as_ref(),
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
            let is_new_key = self
                .last_returned_key
                .as_ref()
                .is_none_or(|last| last != current_key_ref);

            // 6. 核心逻辑分支
            if is_new_key {
                // [关键步骤 A] 无论是否为 Tombstone，必须先记录这个 Key。
                // 这样下一次循环时，才能正确识别并跳过这个 Key 的旧版本。
                self.last_returned_key = Some(current_key_ref.to_vec());

                // [关键步骤 B] 检查 Value 是否为空 (Tombstone)
                if self.inner.value().is_empty() {
                    // 如果是删除标记，绝对不能 return！
                    // 直接 continue，进入下一次 loop。
                    // 下一次 loop 会读到旧版本，但因为 is_new_key 为 false，会被跳过。
                    continue;
                }

                // 只有非空的有效值，才返回给用户
                return Ok(());
            }

            // 如果不是新 Key (旧版本)，隐式 continue，继续找下一个。
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
