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

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice<'_> {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !(self.key.is_empty() || self.idx == usize::MAX)
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        if self.block.offsets.is_empty() {
            self.idx = usize::MAX;
            return;
        }
        self.idx = 0;
        self.decode_entry_at(self.idx);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.idx + 1 < self.block.offsets.len() {
            self.idx += 1;
            self.decode_entry_at(self.idx);
        } else {
            self.idx = usize::MAX; // invalid
        }
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        println!("block len{}", self.block.offsets.len());
        for i in 0..self.block.offsets.len() {
            self.decode_entry_at(i);
            println!("self {:?} key {:?}", self.key.as_key_slice(), key);
            if self.key.as_key_slice() >= key {
                self.idx = i;
                return;
            }
        }
        println!("not found");
        self.idx = usize::MAX; // 没找到，invalid
    }

    fn decode_entry_at(&mut self, idx: usize) {
        if idx == usize::MAX {
            return;
        }
        let start = self.block.offsets[idx] as usize;
        let data = &self.block.data[start..];

        let overlap_len = u16::from_le_bytes([data[0], data[1]]) as usize;
        let rest_len = u16::from_le_bytes([data[2], data[3]]) as usize;
        let rest_key_start = 4;
        let rest_key = &data[rest_key_start..rest_key_start + rest_len];

        if idx == 0 {
            // First key
            self.key = KeyVec::from_vec(rest_key.to_vec());
            self.first_key = self.key.clone();
        } else {
            // Reconstruct key: first_key[..overlap_len] + rest_key
            let mut full_key = self.first_key.as_key_slice().raw_ref()[..overlap_len].to_vec();
            full_key.extend_from_slice(rest_key);
            self.key = KeyVec::from_vec(full_key);
        }

        // 解析 value
        let value_offset = rest_key_start + rest_len;
        let value_len = u16::from_le_bytes([data[value_offset], data[value_offset + 1]]) as usize;
        let value_start = value_offset + 2;
        let value_end = value_start + value_len;

        self.value_range = (start + value_start, start + value_end);
    }
}
