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

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use nom::AsBytes;

use super::{BlockMeta, SsTable};
use crate::{
    block::{self, BlockBuilder},
    key::{Key, KeySlice},
    lsm_storage::BlockCache,
    table::FileObject,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        SsTableBuilder {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        // Try adding the key-value pair to the current block.
        let res = self.builder.add(key, value);
        // If the block is full (i.e., add returns false), finalize it and start a new block.
        if !res {
            // Instead of calling build() directly, we can handle the builder's state manually:
            let block_data = self.builder.build().encode(); // Finalize the block data
            self.data.extend_from_slice(block_data.as_bytes());
            self.meta.push(BlockMeta {
                offset: self.data.len() - block_data.len(),
                first_key: self.builder.first_key(),
                last_key: Key::from_bytes(bytes::Bytes::from(self.last_key.clone().to_vec())),
            });

            // After calling build, reset the builder to avoid moving out of it.
            self.builder = BlockBuilder::new(self.block_size); // Reset the builder for new additions
            let res2 = self.builder.add(key, value);
            assert!(res2);
        }

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec().into_inner();
        }
        self.last_key = key.to_key_vec().into_inner();
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        println!("Building SSTable with {} blocks", self.meta.len() + 1);
        if !self.builder.is_empty() {
            let block_data = self.builder.build().encode(); // Finalize the block data
            self.data.extend_from_slice(block_data.as_bytes());
            self.meta.push(BlockMeta {
                offset: self.data.len() - block_data.len(),
                first_key: self.builder.first_key(),
                last_key: Key::from_bytes(bytes::Bytes::from(self.last_key.clone().to_vec())),
            });
        }

        let mut sstable_data = Vec::new();

        // 拼接 Block Section（数据块部分）
        sstable_data.extend_from_slice(&self.data);
        let mut encoded_mata = Vec::new();
        BlockMeta::encode_block_meta(&self.meta, &mut encoded_mata);
        sstable_data.extend_from_slice(&encoded_mata);

        // 拼接 Meta Block Offset（元数据块偏移量）
        let meta_offset = self.data.len() as u32;
        sstable_data.extend_from_slice(&meta_offset.to_le_bytes());

        let file = FileObject::create(path.as_ref(), sstable_data)?;
        Ok(SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset: meta_offset as usize,
            id,
            block_cache,
            first_key: Key::from_bytes(bytes::Bytes::from(self.first_key.clone().to_vec())),
            last_key: Key::from_bytes(bytes::Bytes::from(self.last_key.clone().to_vec())),
            bloom: None,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
