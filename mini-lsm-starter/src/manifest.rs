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

use std::fs::OpenOptions;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::{Context, Result};
use bytes::Buf;
use crc32fast::Hasher;
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;
use crate::lsm_storage::LsmStorageState;

pub struct Manifest {
    file: Arc<Mutex<File>>,
    compaction_threshold: usize,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),                           // SST ID
    NewMemtable(usize),                     // Memtable ID
    Compaction(CompactionTask, Vec<usize>), // Task and output SST IDs
}

/// we designed the manifest to be a append-only file.
/// The manifest format is like:
/// | JSON record | JSON record | JSON record | JSON record |
/// After adding checksum:
/// | len | JSON record | checksum | len | JSON record | checksum | len | JSON record | checksum |
impl Manifest {
    // Add this constant
    const DEFAULT_COMPACTION_THRESHOLD: usize = 1024 * 1024; // 1MB

    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(_path)
                    .context("failed to create manifest")?,
            )),
            compaction_threshold: Self::DEFAULT_COMPACTION_THRESHOLD,
        })
    }

    pub fn recover(_path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        // Open file for both read and write
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(_path)
            .context("failed to recover manifest")?;

        // Read entire file content
        let mut reader = BufReader::new(&file);
        let mut content = Vec::new();
        reader.read_to_end(&mut content)?;

        // Parse records using streaming deserializer
        let mut records = Vec::new();
        // let mut deserializer = serde_json::Deserializer::from_slice(&content);
        // while let Ok(record) = ManifestRecord::deserialize(&mut deserializer) {
        //     records.push(record);
        // }
        let mut rbuf = content.as_slice();
        while rbuf.remaining() >= 2 * std::mem::size_of::<u32>() {
            // Minimum: length(4) + checksum(4)
            let record_start = content.len() - rbuf.remaining();

            // Read record length
            let record_len = rbuf.get_u32_le() as usize;
            if rbuf.remaining() < record_len + 4 {
                // +4 for checksum
                panic!("Incomplete manifest record"); // Incomplete record
            }

            // Read JSON content
            let json_data = rbuf.copy_to_bytes(record_len);

            // Read checksum
            let stored_checksum = rbuf.get_u32_le();

            // Verify checksum
            let mut hasher = Hasher::new();
            hasher.update(&(record_len as u32).to_le_bytes());
            hasher.update(&json_data);
            let computed_checksum = hasher.finalize();

            if computed_checksum != stored_checksum {
                panic!("manifest record checksum mismatch"); // Corrupted record
            }

            // Parse record
            if let Ok(record) = serde_json::from_slice(&json_data) {
                records.push(record);
            }
        }

        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
                compaction_threshold: Self::DEFAULT_COMPACTION_THRESHOLD,
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, _record: ManifestRecord) -> Result<()> {
        let mut file = self.file.lock();

        // Seek to end of file
        file.seek(SeekFrom::End(0))?;

        // Serialize and write record
        let json = serde_json::to_vec(&_record)?;

        // Calculate total length needed (length + json + checksum)
        let record_len = json.len();

        // Write record length (4 bytes)
        file.write_all(&(record_len as u32).to_le_bytes())?;
        // Write JSON content
        file.write_all(&json)?;

        // Calculate and write checksum
        let mut hasher = Hasher::new();
        hasher.update(&(record_len as u32).to_le_bytes());
        hasher.update(&json);
        let checksum = hasher.finalize();
        file.write_all(&checksum.to_le_bytes())?;

        // Ensure record is written to disk
        file.sync_all()?;

        Ok(())
    }

    /// Manifest Compaction. When the number of logs in the manifest file gets too large,
    /// you can rewrite the manifest file to only store the current snapshot and append new logs to that file.
    pub fn compaction(&self, _path: impl AsRef<Path>, _snapshot: &LsmStorageState) -> Result<Self> {
        // First check the file size
        let file_size = std::fs::metadata(&_path)?.len() as usize;
        if file_size < self.compaction_threshold {
            return Ok(Self {
                file: Arc::clone(&self.file),
                compaction_threshold: self.compaction_threshold,
            });
        }

        // Create temporary manifest file
        let temp_path = _path.as_ref().with_extension("manifest.tmp");
        let mut temp_file = File::create(&temp_path)?;

        // Write minimum necessary records for current snapshot

        // 1. Write records for L0 SSTs
        for &sst_id in &_snapshot.l0_sstables {
            let record = ManifestRecord::Flush(sst_id);
            let json = serde_json::to_vec(&record)?;
            temp_file.write_all(&json)?;
        }

        // 2. Write records for leveled SSTs
        for (level, sst_ids) in &_snapshot.levels {
            for &sst_id in sst_ids {
                let record = ManifestRecord::Flush(sst_id);
                let json = serde_json::to_vec(&record)?;
                temp_file.write_all(&json)?;
            }
        }

        // Ensure all records are written to disk
        temp_file.sync_all()?;

        // Atomically replace old manifest with new one
        std::fs::rename(temp_path, &_path)?;

        // Create new manifest with the compacted file
        Ok(Self {
            file: Arc::new(Mutex::new(
                File::options().read(true).write(true).open(_path)?,
            )),
            compaction_threshold: self.compaction_threshold,
        })
    }

    // Add a method to change the threshold
    pub fn set_compaction_threshold(&mut self, threshold: usize) {
        self.compaction_threshold = threshold;
    }
}
