// REMOVE THIS LINE after fully implementing this functionality
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

use anyhow::{Result, bail};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::key::{KeyBytes, KeySlice};

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    /// WAL format:
    /// | key_len (exclude ts len) (u16) | key | ts (u64) | value_len (u16) | value | checksum (u32) |
    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let path = path.as_ref();
        let mut file = File::options().read(true).append(true).open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut ptr = 0;

        while ptr < buf.len() {
            // Read key_len (u16)
            if ptr + 2 > buf.len() {
                break;
            }
            let key_len = u16::from_be_bytes(buf[ptr..ptr + 2].try_into().unwrap()) as usize;
            ptr += 2;

            // Read key
            if ptr + key_len > buf.len() {
                break;
            }
            let key = &buf[ptr..ptr + key_len];
            ptr += key_len;

            // Read ts (u64)
            if ptr + 8 > buf.len() {
                break;
            }
            let ts = u64::from_be_bytes(buf[ptr..ptr + 8].try_into().unwrap());
            ptr += 8;

            // Read value_len (u16)
            if ptr + 2 > buf.len() {
                break;
            }
            let value_len = u16::from_be_bytes(buf[ptr..ptr + 2].try_into().unwrap()) as usize;
            ptr += 2;

            // Read value
            if ptr + value_len > buf.len() {
                break;
            }
            let value = &buf[ptr..ptr + value_len];
            ptr += value_len;

            // Read checksum (u32)
            if ptr + 4 > buf.len() {
                break;
            }
            let stored_checksum = u32::from_be_bytes(buf[ptr..ptr + 4].try_into().unwrap());
            ptr += 4;

            // Compute checksum over key_len, key, ts, value_len, value
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&(key_len as u16).to_be_bytes());
            hasher.update(key);
            hasher.update(&ts.to_be_bytes());
            hasher.update(&(value_len as u16).to_be_bytes());
            hasher.update(value);
            let computed_checksum = hasher.finalize();

            if stored_checksum != computed_checksum {
                bail!(
                    "WAL checksum mismatch: stored={}, computed={}",
                    stored_checksum,
                    computed_checksum
                );
            }

            // Insert into skiplist
            let key_bytes = KeyBytes::from_bytes_with_ts(Bytes::copy_from_slice(key), ts);
            skiplist.insert(key_bytes, Bytes::copy_from_slice(value));
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    /// WAL format:
    /// | key_len (exclude ts len) (u16) | key | ts (u64) | value_len (u16) | value | checksum (u32) |
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();

        let key_data = key.key_ref();
        let ts = key.ts();

        // Compute checksum
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&(key_data.len() as u16).to_be_bytes());
        hasher.update(key_data);
        hasher.update(&ts.to_be_bytes());
        hasher.update(&(value.len() as u16).to_be_bytes());
        hasher.update(value);
        let checksum = hasher.finalize();

        // Write key_len (u16)
        file.write_all(&(key_data.len() as u16).to_be_bytes())?;
        // Write key
        file.write_all(key_data)?;
        // Write ts (u64)
        file.write_all(&ts.to_be_bytes())?;
        // Write value_len (u16)
        file.write_all(&(value.len() as u16).to_be_bytes())?;
        // Write value
        file.write_all(value)?;
        // Write checksum (u32)
        file.write_all(&checksum.to_be_bytes())?;

        Ok(())
    }

    /// Implement this in week 3, day 5; if you want to implement this earlier, use `&[u8]` as the key type.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
