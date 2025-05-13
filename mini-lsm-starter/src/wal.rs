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
use crc32fast::Hasher;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::key::{KeyBytes, KeySlice};

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

/// The WAL encoding is simply a list of key-value pairs.
/// | raw_len | key | ts(u64) | value_len | value | checksum (u32) |
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

    pub fn recover(_path: impl AsRef<Path>, _skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
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
        while rbuf.remaining() >= 3 * std::mem::size_of::<u32>() {
            // Minimum size: key_len(4) + value_len(4) + checksum(4)
            let record_start = buf.len() - rbuf.remaining();

            // Read raw_len (key + timestamp length) and validate
            let raw_len = rbuf.get_u32_le() as usize;
            let key_len = raw_len - std::mem::size_of::<u64>();

            if rbuf.remaining() < raw_len + std::mem::size_of::<u32>() {
                panic!("WAL recover read key and timestamp failed");
            }

            // Read key bytes
            let key = rbuf.copy_to_bytes(key_len);
            // Read timestamp
            let ts = rbuf.get_u64_le();

            // Read value length and validate
            // Make sure we have enough bytes for the value length
            let value_len = rbuf.get_u32_le() as usize;
            if rbuf.remaining() < value_len + std::mem::size_of::<u32>() {
                // +4 for checksum
                panic!("WAL recover read value failed");
            }

            // Read value
            let value = rbuf.copy_to_bytes(value_len);

            // Calculate length of data for checksum (key_len + key + value_len + value)
            let data_len =
                std::mem::size_of::<u32>() + raw_len + std::mem::size_of::<u32>() + value_len;

            // Read and verify checksum
            let stored_checksum = rbuf.get_u32_le();

            // Calculate checksum of the record
            let mut hasher = Hasher::new();
            hasher.update(&buf[record_start..record_start + data_len]);
            let computed_checksum = hasher.finalize();

            if computed_checksum != stored_checksum {
                panic!("WAL record checksum mismatch"); // Stop at corrupted record
            }

            // Insert into skiplist
            _skiplist.insert(KeyBytes::from_bytes_with_ts(key, ts), value);
        }

        // Create WAL instance with the opened file
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, _key: KeySlice, _value: &[u8]) -> Result<()> {
        // Reuse put_batch for single put operation
        self.put_batch(&[(_key, _value)])
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut writer = self.file.lock();
        // Calculate total capacity needed
        let total_capacity: usize = _data
            .iter()
            .map(|(key, value)| {
                std::mem::size_of::<u32>() + // key length
                key.raw_len() +                   // key bytes + timestamp
                std::mem::size_of::<u32>() + // value length
                value.len() + // value bytes
                std::mem::size_of::<u32>() // checksum
            })
            .sum();

        let mut buf = BytesMut::with_capacity(total_capacity);

        for (key, value) in _data {
            let start_pos = buf.len();

            // Write raw_len (key + timestamp length)
            buf.put_u32_le(key.raw_len() as u32);
            // Write key and timestamp
            buf.put_slice(key.key_ref());
            buf.put_u64_le(key.ts());

            // Write value length and value
            buf.put_u32_le(value.len() as u32);
            buf.put_slice(value);

            // Calculate and write checksum
            let mut hasher = Hasher::new();
            hasher.update(&buf[start_pos..buf.len()]);
            let checksum = hasher.finalize();
            buf.put_u32_le(checksum);
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
