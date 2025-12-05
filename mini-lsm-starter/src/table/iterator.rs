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

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block = table.read_block_cached(0)?; // Read the first block
        let iterator = SsTableIterator {
            table,
            blk_iter: BlockIterator::create_and_seek_to_first(block), // Start with the first block
            blk_idx: 0,
        };
        Ok(iterator)
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.blk_idx = 0;
        let block = self.table.read_block_cached(self.blk_idx)?; // Read the first block
        self.blk_iter = BlockIterator::create_and_seek_to_first(block); // Start with the first block
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let mut blk_idx = table.find_block_idx(key);
        let block = table.read_block_cached(blk_idx)?; // Read the appropriate block
        let mut blk_iter = BlockIterator::create_and_seek_to_key(block, key); // Start with the found block

        if !blk_iter.is_valid() {
            blk_idx += 1; // 尝试下一个块

            // 确保没有越界
            if blk_idx < table.num_of_blocks() {
                // 读取下一个 Block
                blk_iter =
                    BlockIterator::create_and_seek_to_key(table.read_block_cached(blk_idx)?, key);
            }
        }
        Ok(Self {
            table,
            blk_iter,
            blk_idx,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let blk_idx = self.table.find_block_idx(key);
        println!("seek_to_key: found block index {}", blk_idx);

        // 如果需要切换到不同的块
        if blk_idx != self.blk_idx {
            self.blk_idx = blk_idx;
            let block = self.table.read_block_cached(self.blk_idx)?; // Read the appropriate block
            self.blk_iter = BlockIterator::create_and_seek_to_key(block, key);
        } else {
            // 在当前块中查找
            self.blk_iter.seek_to_key(key);
            if self.blk_iter.is_valid() {
                return Ok(());
            } else {
                // 如果在当前块中没有找到，尝试移动到下一个块
                self.blk_idx += 1;
                if self.blk_idx >= self.table.block_meta.len() {
                    // End of table reached
                    return Ok(());
                }
                let next_block = self.table.read_block_cached(self.blk_idx)?;
                self.blk_iter = BlockIterator::create_and_seek_to_key(next_block, key);
            }
        }

        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&'_ self) -> KeySlice<'_> {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();

        if self.blk_iter.is_valid() {
            Ok(())
        } else {
            // If we reach the end of the current block, move to the next block.
            self.blk_idx += 1;
            if self.blk_idx >= self.table.block_meta.len() {
                // End of table reached
                return Ok(());
            }
            let next_block = self.table.read_block_cached(self.blk_idx)?;
            self.blk_iter = BlockIterator::create_and_seek_to_first(next_block);
            Ok(())
        }
    }
}
