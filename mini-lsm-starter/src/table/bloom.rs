// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use anyhow::{bail, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Implements a bloom filter
pub struct Bloom {
    /// data of filter in bits
    pub(crate) filter: Bytes,
    /// number of hash functions
    pub(crate) k: u8,
}

// 布隆过滤器结构体(Bloom)
// 字段：
// filter: 存储过滤器数据的Bytes类型，实际上是一个位数组。
// k: 使用的哈希函数的数量。
// 特征实现(Trait)
// BitSlice和BitSliceMut：为支持的类型提供了读取和修改位数组的能力。这两个trait通过扩展AsRef<[u8]>和AsMut<[u8]>的类型，使得任何可以转换为字节切片的类型都能使用这些功能。
// get_bit: 根据索引获取位的值。
// set_bit: 根据索引设置位的值。
// bit_len: 返回位数组的长度。
// 方法
// decode: 解码一个布隆过滤器实例。这个方法首先验证数据的CRC32校验和以确保数据的完整性，然后从字节缓冲区中提取过滤器数据和k值。

// encode: 将布隆过滤器实例编码到一个Vec<u8>缓冲区。它将过滤器数据和k值追加到缓冲区，并计算追加这些数据后的CRC32校验和，最后将校验和追加到缓冲区。

// may_contain: 检查一个给定哈希值的数据是否可能存在于布隆过滤器中。这个方法根据k值和哈希值计算位位置，如果所有计算的位置都被设置，则认为数据可能存在。

// 使用场景
// 布隆过滤器广泛应用于数据库和缓存系统中，用于快速判断一个元素是否存在于一个很大的集合中，特别是在查找操作远多于插入操作的场景。通过使用布隆过滤器，可以显著减少对磁盘或远程服务的访问次数，从而提高性能。不过，设计布隆过滤器时需要仔细选择k值和位数组的大小，以平衡空间使用和假阳性率。
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
        let checksum = (&buf[buf.len() - 4..buf.len()]).get_u32();
        if checksum != crc32fast::hash(&buf[..buf.len() - 4]) {
            bail!("checksum mismatched for bloom filters");
        }
        let filter = &buf[..buf.len() - 5];
        let k = buf[buf.len() - 5];
        Ok(Self {
            filter: filter.to_vec().into(),
            k,
        })
    }

    /// Encode a bloom filter
    pub fn encode(&self, buf: &mut Vec<u8>) {
        let offset = buf.len();
        buf.extend(&self.filter);
        buf.put_u8(self.k);
        let checksum = crc32fast::hash(&buf[offset..]);
        buf.put_u32(checksum);
    }
    // bloom_bits_per_key: 根据给定的元素数量和期望的假阳性率计算每个键需要的位数。

    // build_from_key_hashes: 根据一组键的哈希值和每个键需要的位数构建一个新的布隆过滤器。
    //这个方法根据k值循环添加每个键的哈希值到过滤器中。

    /// Get bloom filter bits per key from entries count and FPR
    pub fn bloom_bits_per_key(entries: usize, false_positive_rate: f64) -> usize {
        let size =
            -1.0 * (entries as f64) * false_positive_rate.ln() / std::f64::consts::LN_2.powi(2);
        let locs = (size / (entries as f64)).ceil();
        locs as usize
    }

    /// Build bloom filter from key hashes
    pub fn build_from_key_hashes(keys: &[u32], bits_per_key: usize) -> Self {
        let k = (bits_per_key as f64 * 0.69) as u32;
        let k = k.min(30).max(1);
        let nbits = (keys.len() * bits_per_key).max(64);
        let nbytes = (nbits + 7) / 8;
        let nbits = nbytes * 8;
        let mut filter = BytesMut::with_capacity(nbytes);
        filter.resize(nbytes, 0);
        for h in keys {
            let mut h = *h;
            let delta = (h >> 17) | (h << 15);
            for _ in 0..k {
                let bit_pos = (h as usize) % nbits;
                filter.set_bit(bit_pos, true);
                h = h.wrapping_add(delta);
            }
        }
        Self {
            filter: filter.freeze(),
            k: k as u8,
        }
    }
    // may_contain: 检查一个给定哈希值的数据是否可能存在于布隆过滤器中。
    //这个方法根据k值和哈希值计算位位置，如果所有计算的位置都被设置，则认为数据可能存在。

    /// Check if a bloom filter may contain some data
    /// Check if a bloom filter may contain some data
    pub fn may_contain(&self, mut h: u32) -> bool {
        if self.k > 30 {
            // potential new encoding for short bloom filters
            true
        } else {
            let nbits = self.filter.bit_len();
            let delta = (h >> 17) | (h << 15);
            for _ in 0..self.k {
                let bit_pos = h % (nbits as u32);
                if !self.filter.get_bit(bit_pos as usize) {
                    return false;
                }
                h = h.wrapping_add(delta);
            }
            true
        }
    }
}
