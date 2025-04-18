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

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

/// The WAL encoding is simply a list of key-value pairs.
/// | key_len | key | value_len | value |
impl Wal {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        // File::create 的设计意图是创建一个新的文件，或者替换一个已存在的文件。 为了确保创建一个干净的状态，它会先清空文件内容
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(_path)
                    .context("failed to create WAL")?,
            ))),
        })
    }

    pub fn recover(_path: impl AsRef<Path>, _skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        // Open the file in read-write mode
        let file = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .open(&_path)?;

        // Read entire file content
        let mut buf = Vec::new();
        let mut reader = std::io::BufReader::new(&file);
        reader.read_to_end(&mut buf)?;
        let mut rbuf = buf.as_slice();

        // Read all records from the buffer
        while rbuf.has_remaining() {
            // Make sure we have enough bytes for the key length
            if rbuf.remaining() < 4 {
                break;
            }

            // Read key length and validate
            let key_len = rbuf.get_u32_le() as usize;
            if rbuf.remaining() < key_len {
                break;
            }

            // Read key
            let key = &rbuf[..key_len];
            rbuf.advance(key_len);

            // Make sure we have enough bytes for the value length
            if rbuf.remaining() < 4 {
                break;
            }

            // Read value length and validate
            let value_len = rbuf.get_u32_le() as usize;
            if rbuf.remaining() < value_len {
                break;
            }

            // Read value
            let value = &rbuf[..value_len];
            rbuf.advance(value_len);

            // Insert into skiplist
            _skiplist.insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        }

        // Create WAL instance with the opened file
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        // Reuse put_batch for single put operation
        self.put_batch(&[(_key, _value)])
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(&[u8], &[u8])]) -> Result<()> {
        let mut writer = self.file.lock();
        // Calculate total capacity needed
        let total_capacity: usize = _data
            .iter()
            .map(|(key, value)| {
                std::mem::size_of::<u32>() + // key length
            key.len() +                   // key bytes
            std::mem::size_of::<u32>() + // value length
            value.len() // value bytes
            })
            .sum();

        let mut buf = BytesMut::with_capacity(total_capacity);

        for (key, value) in _data {
            // Write key length and key
            buf.put_u32_le(key.len() as u32);
            buf.put_slice(key);

            // Write value length and value
            buf.put_u32_le(value.len() as u32);
            buf.put_slice(value);
        }
        // Write the entire buffer to file
        writer.write_all(&buf)?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut writer = self.file.lock();
        // Flush buffered data to the file
        writer.flush()?;
        // Ensure data is persisted to disk
        writer.get_mut().sync_all()?;
        Ok(())
    }
}
