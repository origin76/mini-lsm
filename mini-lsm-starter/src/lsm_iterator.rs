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
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, end_bound: Bound<Bytes>) -> Result<Self> {
        let mut it = Self {
            is_valid: iter.is_valid(),
            inner: iter,
            end_bound,
        };

        while it.is_valid && it.inner.value().is_empty() {
            it.next()?;
        }

        // Clamp to the user-specified upper bound if necessary.
        if !matches!(it.end_bound, Bound::Unbounded) && it.is_valid() {
            if !it.check_end_bound() {
                it.is_valid = false;
            }
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
            self.inner.next()?; // 推进到底层

            if !self.inner.is_valid() {
                self.is_valid = false;
                return Ok(());
            }

            // 检查是否超出 end_bound
            if !self.is_valid() {
                self.is_valid = false;
                return Ok(());
            }

            if !self.inner.value().is_empty() {
                return Ok(());
            }
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
