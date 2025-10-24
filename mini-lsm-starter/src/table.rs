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

pub(crate) mod bloom;
mod builder;
pub(crate) mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::{Buf, Bytes};
pub use iterator::SsTableIterator;
use nom::AsBytes;

use crate::block::Block;
use crate::key::{Key, KeyBytes, KeySlice, KeyVec};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        for meta in block_meta {
            buf.extend_from_slice(&meta.offset.to_le_bytes());

            buf.extend_from_slice(&(meta.first_key.len() as u16).to_le_bytes());
            buf.extend_from_slice(&meta.first_key.raw_ref());

            buf.extend_from_slice(&(meta.last_key.len() as u16).to_le_bytes());
            buf.extend_from_slice(&meta.last_key.raw_ref());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(buf: impl Buf) -> Vec<BlockMeta> {
        let mut buf = buf;
        let mut block_metas = Vec::new();

        while buf.has_remaining() {
            // 读取 offset
            let offset = buf.get_u64_le() as usize; // 读取 8 个字节，作为 u64
            println!("Decoding BlockMeta at offset: {}", offset);

            // 读取 first_key 的长度并读取数据
            let first_key_len = buf.get_u16_le() as usize;
            let first_key = buf.copy_to_bytes(first_key_len).to_vec();
            let first_key = Key::from_bytes(bytes::Bytes::from(first_key.clone()));

            // 读取 last_key 的长度并读取数据
            let last_key_len = buf.get_u16_le() as usize;
            let last_key = buf.copy_to_bytes(last_key_len).to_vec();
            let last_key = Key::from_bytes(bytes::Bytes::from(last_key.clone()));

            // 创建 BlockMeta
            block_metas.push(BlockMeta {
                offset,
                first_key,
                last_key,
            });
        }

        block_metas
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let bloom_offset_offset = file.size() - 4;
        let bloom_offset_bytes = file.read(bloom_offset_offset, 4)?;
        let bloom_offset = u32::from_le_bytes(bloom_offset_bytes.try_into().unwrap()) as u64;
        let bloom_len = file.size() - 4 - bloom_offset;
        let bloom = SsTable::read_bloom(&file, bloom_offset, bloom_len)?;

        let meta_offset_offset = bloom_offset as u64 - 4;
        let meta_offset_bytes = file.read(meta_offset_offset as u64, 4)?;
        let block_meta_offset = u32::from_le_bytes(meta_offset_bytes.try_into().unwrap()) as u64;
        let block_meta_len = bloom_offset - 4 - block_meta_offset;

        let block_meta = SsTable::read_block_meta(&file, block_meta_offset, block_meta_len)?;

        // 计算SSTable的第一个和最后一个键
        let first_key = block_meta
            .get(0)
            .map_or(KeyBytes::default(), |meta| meta.first_key.clone());
        let last_key = block_meta
            .last()
            .map_or(KeyBytes::default(), |meta| meta.last_key.clone());

        // 构造并返回 SsTable 对象
        Ok(SsTable {
            file,
            block_meta,
            block_meta_offset: block_meta_offset as usize,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom), // Bloom filter can be handled later
            max_ts: 0,          // You can compute this during the iteration phase
        })
    }

    fn read_block_meta(file: &FileObject, offset: u64, length: u64) -> Result<Vec<BlockMeta>> {
        let buf = file.read(offset, length)?;
        Ok(BlockMeta::decode_block_meta(bytes::Bytes::from(buf)))
    }

    fn read_bloom(file: &FileObject, offset: u64, length: u64) -> Result<Bloom> {
        let buf = file.read(offset, length)?;
        let res = Bloom::decode(buf.as_bytes())?;
        Ok(res)
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let meta = &self.block_meta[block_idx];
        let len;
        if block_idx < self.block_meta.len() - 1 {
            len = (self.block_meta[block_idx + 1].offset - meta.offset) as u64;
        } else if block_idx == self.block_meta.len() - 1 {
            len = (self.block_meta_offset - meta.offset) as u64;
        } else {
            return Err(anyhow::anyhow!("block_idx out of range"));
        }
        // 根据块元数据中的偏移量读取数据块
        let block_data = self.file.read(meta.offset as u64, len)?;
        let block = Block::decode(&block_data);
        Ok(Arc::new(block))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        unimplemented!()
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        println!(
            "find_block_idx: searching for key {:?} in SSTable {}",
            key, self.id
        );
        let meta = self
            .block_meta
            .binary_search_by(|m| m.first_key.as_key_slice().cmp(&key));
        match meta {
            Ok(idx) => idx,
            Err(0) => {
                println!("find_block_idx: key is smaller than the first key in the table");
                0
            }
            Err(idx) => {
                println!("find_block_idx: key is not found, return the previous block");
                idx - 1
            }
        }
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }

    /// Get a key from the SSTable.
    pub fn get(&self, key: KeySlice) -> Result<Option<(KeyVec, Bytes)>> {
        // First check if key is within table range
        if key < self.first_key().as_key_slice() || key > self.last_key().as_key_slice() {
            return Ok(None);
        }

        // Find the block that may contain the key
        let block_idx = self.find_block_idx(key);

        if block_idx >= self.num_of_blocks() {
            return Ok(None);
        }

        // Read the block
        let block = self.read_block(block_idx)?;

        // Search for the key in the block
        let iterator = crate::block::BlockIterator::create_and_seek_to_key(block, key);

        // Find the exact key
        if iterator.is_valid() && iterator.key() == key {
            return Ok(Some((
                iterator.key().to_key_vec(),
                Bytes::copy_from_slice(iterator.value()),
            )));
        }
        Ok(None)
    }
}
