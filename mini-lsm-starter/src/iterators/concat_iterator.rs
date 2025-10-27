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

use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables: vec![],
            });
        }

        let mut iter = SstConcatIterator {
            current: None,
            next_sst_idx: 0,
            sstables,
        };

        iter.seek_to_first()?;

        Ok(iter)
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables: vec![],
            });
        }
        let mut iter = SstConcatIterator {
            current: None,
            next_sst_idx: 0,
            sstables,
        };

        iter.seek_to_key(key)?;

        Ok(iter)
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.next_sst_idx = 0;
        self.current = Some(SsTableIterator::create_and_seek_to_first(
            self.sstables[self.next_sst_idx].clone(),
        )?);
        Ok(())
    }

    fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        println!("concat to key {:?}", key.raw_ref());

        // 情况 1: key 小于全局最小 key
        if key < self.sstables[0].first_key().as_key_slice() {
            let iter = SsTableIterator::create_and_seek_to_first(self.sstables[0].clone())?;
            self.current = Some(iter);
            self.next_sst_idx = 0;
            return Ok(());
        }

        // 情况 2: key 在范围中
        for idx in 0..self.sstables.len() {
            let sst = &self.sstables[idx];
            if sst.first_key().as_key_slice() <= key && key <= sst.last_key().as_key_slice() {
                let iter = SsTableIterator::create_and_seek_to_key(sst.clone(), key)?;
                if iter.is_valid() {
                    self.current = Some(iter);
                    self.next_sst_idx = idx;
                    return Ok(());
                }
            }
        }

        // 情况 3: key 大于全局最大 key → 设为 invalid
        self.current = None;
        self.next_sst_idx = self.sstables.len();
        Ok(())
    }

    fn advance_to_next_sst(&mut self) -> Result<()> {
        self.next_sst_idx += 1;

        if self.next_sst_idx < self.sstables.len() {
            self.current = Some(SsTableIterator::create_and_seek_to_first(
                self.sstables[self.next_sst_idx].clone(),
            )?);
            self.current.as_mut().unwrap().seek_to_first()?;
        } else {
            self.current = None; // 所有 SST 文件迭代完毕
        }

        Ok(())
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice<'_> {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        if let Some(iter) = self.current.as_ref() {
            self.current.as_ref().unwrap().is_valid()
        } else {
            false
        }
    }

    fn next(&mut self) -> Result<()> {
        if let Some(iter) = self.current.as_mut() {
            iter.next()?;
            if !iter.is_valid() {
                self.advance_to_next_sst()?;
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
