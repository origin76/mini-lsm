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

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::key::KeySlice;

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

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let path = path.as_ref();
        let mut file = File::options().read(true).append(true).open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut ptr = 0;
        while ptr < buf.len() {
            if ptr + 4 > buf.len() {
                break;
            }
            let key_len = u32::from_be_bytes(buf[ptr..ptr + 4].try_into().unwrap()) as usize;
            ptr += 4;
            if ptr + key_len > buf.len() {
                break;
            }
            let key = Bytes::copy_from_slice(&buf[ptr..ptr + key_len]);
            ptr += key_len;
            if ptr + 4 > buf.len() {
                break;
            }
            let val_len = u32::from_be_bytes(buf[ptr..ptr + 4].try_into().unwrap()) as usize;
            ptr += 4;
            if ptr + val_len > buf.len() {
                break;
            }
            let value = Bytes::copy_from_slice(&buf[ptr..ptr + val_len]);
            ptr += val_len;
            skiplist.insert(key, value);
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        file.write_all(&(key.len() as u32).to_be_bytes())?;
        file.write_all(key)?;
        file.write_all(&(value.len() as u32).to_be_bytes())?;
        file.write_all(value)?;
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
