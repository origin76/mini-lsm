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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{BufMut, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf = Vec::with_capacity(self.data.len() + self.offsets.len() * 2 + 2);

        // 1. 写 data 部分
        buf.extend_from_slice(&self.data);

        // 2. 写 offsets 部分
        for &off in &self.offsets {
            buf.put_u16_le(off);
        }

        // 3. 写 count
        buf.put_u16_le(self.offsets.len() as u16);

        Bytes::from(buf)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let count_pos = data.len() - 2;
        let count = u16::from_le_bytes([data[count_pos], data[count_pos + 1]]) as usize;

        // 2. 解析 offsets
        let offsets_pos = count_pos - count * 2;
        let mut offsets = Vec::with_capacity(count);
        for i in 0..count {
            let start = offsets_pos + i * 2;
            let off = u16::from_le_bytes([data[start], data[start + 1]]);
            offsets.push(off);
        }

        // 3. 拷贝 data 部分
        let data_section = data[..offsets_pos].to_vec();

        Block {
            data: data_section,
            offsets,
        }
    }
}
