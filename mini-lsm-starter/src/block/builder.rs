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

use bytes::{BufMut, Bytes};

use crate::key::{Key, KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    pub fn first_key(&self) -> Key<Bytes> {
        self.first_key.clone().into_key_bytes()
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let offset = self.data.len() as u16;

        let key_len = key.len() as u16;
        let value_len = value.len() as u16;

        let entry_size = key_len + value_len + 6;

        if offset + entry_size > self.block_size as u16 {
            if !self.first_key.is_empty() {
                return false;
            }
        }

        if self.offsets.is_empty() {
            self.first_key = key.to_key_vec();
        }

        // 写入 offsets
        self.offsets.push(offset);

        self.data.put_u16_le(key_len);
        self.data.put_slice(key.raw_ref());
        self.data.put_u16_le(value_len);
        self.data.put_slice(value);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(&mut self) -> Block {
        let num_of_elements = self.offsets.len() as u16;

        // 把 offset section 写入 data 末尾
        for &offset in &self.offsets {
            self.data.put_u16(offset);
        }

        // 写入 num_of_elements
        self.data.put_u16(num_of_elements);

        Block {
            data: self.data.clone(),
            offsets: self.offsets.clone(),
        }
    }
}
