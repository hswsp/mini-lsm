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

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use crc32fast::Hasher;

/// Implements a bloom filter
pub struct Bloom {
    /// data of filter in bits
    /// 假设有一个 16 位的布隆过滤器：
    /// filter[0 1 1 0 0 1 0 0 | 1 0 0 1 0 0 1 0]
    ///              第一个字节         第二个字节
    /// 实际存储为两个字节：[0b01100100, 0b10010010]
    pub(crate) filter: Bytes,
    /// number of hash functions
    pub(crate) k: u8,
}

pub trait BitSlice {
    fn get_bit(&self, idx: usize) -> bool;
    fn bit_len(&self) -> usize;
}

pub trait BitSliceMut {
    fn set_bit(&mut self, idx: usize, val: bool);
}

impl<T: AsRef<[u8]>> BitSlice for T {
    fn get_bit(&self, idx: usize) -> bool {
        let pos = idx / 8;
        let offset = idx % 8;
        (self.as_ref()[pos] & (1 << offset)) != 0
    }

    fn bit_len(&self) -> usize {
        self.as_ref().len() * 8
    }
}

impl<T: AsMut<[u8]>> BitSliceMut for T {
    fn set_bit(&mut self, idx: usize, val: bool) {
        let pos = idx / 8;
        let offset = idx % 8;
        if val {
            self.as_mut()[pos] |= 1 << offset;
        } else {
            self.as_mut()[pos] &= !(1 << offset);
        }
    }
}

impl Bloom {
    /// Decode a bloom filter
    pub fn decode(buf: &[u8]) -> Result<Self> {
        let checksum_pos = buf.len() - 4;

        // 从输入字节切片中分离出过滤器数据和k值
        // filter 是除最后一个字节外的所有数据
        let k_pos = checksum_pos - 1;
        let filter_data = &buf[..k_pos];
        // k值存储在最后一个字节
        let k = buf[k_pos];

        // Verify checksum
        let stored_checksum = u32::from_le_bytes(buf[checksum_pos..].try_into().unwrap());

        // Calculate checksum of filter data and k
        let mut hasher = Hasher::new();
        hasher.update(filter_data);
        hasher.update(&[k]);
        let computed_checksum = hasher.finalize();

        if computed_checksum != stored_checksum {
            return Err(anyhow::anyhow!("Bloom filter checksum mismatch"));
        }

        Ok(Self {
            filter: filter_data.to_vec().into(),
            k,
        })
    }

    /// Encode a bloom filter
    pub fn encode(&self, buf: &mut Vec<u8>) {
        // Record start position for checksum calculation
        let start_pos = buf.len();

        buf.extend(&self.filter);
        // 将哈希函数数量 k 作为单个字节追加到缓冲区末尾
        buf.put_u8(self.k);

        // Calculate checksum for the bloom filter content
        let mut hasher = Hasher::new();
        hasher.update(&buf[start_pos..]); // Hash both filter and k
        let checksum = hasher.finalize();

        // Append checksum
        buf.extend_from_slice(&checksum.to_le_bytes());
    }

    /// Get bloom filter bits per key from entries count and FPR
    /// FPR 约等于 (1 - e^(-k*n/m))^k
    /// k 是哈希函数数量
    /// n 是插入元素数量
    /// m 是总比特数 (n * bits_per_key)
    /// => m = -k*n / ln(1 - FPR^(1/k))
    /// entries: 预期要插入的元素数量
    pub fn bloom_bits_per_key(entries: usize, false_positive_rate: f64) -> usize {
        // false_positive_rate.ln(): 计算误报率的自然对数
        // 需要的bit数 m = -entries*ln(p)/(ln(2))²
        let size =
            -1.0 * (entries as f64) * false_positive_rate.ln() / std::f64::consts::LN_2.powi(2);
        // 将总位数除以元素数量，得到每个键需要的位数
        let locs = (size / (entries as f64)).ceil();
        locs as usize
    }

    /// Build bloom filter from key hashes
    pub fn build_from_key_hashes(keys: &[u32], bits_per_key: usize) -> Self {
        // 计算最优的哈希函数数量 k
        // 0.69 是 ln(2) 的近似值，这是布隆过滤器的最优 k 值计算公式
        let k = (bits_per_key as f64 * 0.69) as u32;
        let k = k.clamp(1, 30);
        // 计算总bit数，至少为 64 位
        let nbits = (keys.len() * bits_per_key).max(64);
        // 计算需要的字节数，向上取整
        let nbytes = (nbits + 7) / 8;
        let nbits = nbytes * 8;
        // 创建指定容量的字节缓冲区
        let mut filter = BytesMut::with_capacity(nbytes);
        filter.resize(nbytes, 0);

        // TODO: build the bloom filter
        for h in keys {
            let mut h = *h;
            // 计算增量值，通过左旋 15 位
            // 用于双重哈希法生成多个哈希函数
            let delta = h.rotate_left(15);
            for _ in 0..k {
                // Use double hashing to generate k hash functions
                let bit_pos = (h as usize) % nbits;
                filter.set_bit(bit_pos, true);
                // 更新哈希值，使用环绕加法避免溢出
                // 用于处理整数溢出的安全加法操作
                h = h.wrapping_add(delta);
            }
        }

        Self {
            filter: filter.freeze(),
            k: k as u8,
        }
    }

    /// Check if a bloom filter may contain some data
    pub fn may_contain(&self, h: u32) -> bool {
        if self.k > 30 {
            // potential new encoding for short bloom filters
            true
        } else {
            let nbits = self.filter.bit_len();
            let delta = h.rotate_left(15);

            // TODO: probe the bloom filter
            let mut h = h;
            for _ in 0..self.k {
                let bit_pos = (h as usize) % nbits;
                if !self.filter.get_bit(bit_pos) {
                    return false; // If any bit is not set, key definitely not present
                }
                h = h.wrapping_add(delta);
            }

            true
        }
    }
}
