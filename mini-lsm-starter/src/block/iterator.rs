#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::Buf;
use std::sync::Arc;

use crate::{
    block::SIZEOF_U16,
    key::{KeySlice, KeyVec},
};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the value range from the block
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl Block {
    fn get_first_key(&self) -> KeyVec {
        let mut buf = &self.data[..];
        buf.get_u16();
        let key_len = buf.get_u16();
        let key = &buf[..key_len as usize];
        KeyVec::from_vec(key.to_vec())
    }
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            key: KeyVec::new(),
            first_key: block.get_first_key(),
            block,
            value_range: (0, 0),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }
    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to(0);
    }
    /// Seeks to the idx-th key in the block.
    fn seek_to(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }
        let offset = self.block.offsets[idx] as usize;
        self.seek_to_offset(offset);
        self.idx = idx;
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        self.seek_to(self.idx);
    }

    /// Seek to the specified position and update the current `key` and `value`
    /// Index update will be handled by caller
    fn seek_to_offset(&mut self, offset: usize) {
        let mut entry = &self.block.data[offset..];
        // Since `get_u16()` will automatically move the ptr 2 bytes ahead here,
        // we don't need to manually advance it
        let overlap_len = entry.get_u16() as usize;
        let key_len = entry.get_u16() as usize;
        let key = &entry[..key_len];
        self.key.clear();
        println!("overlap {:#?}", overlap_len);
        self.key.append(&self.first_key.raw_ref()[..overlap_len]);
        self.key.append(key);
        entry.advance(key_len);
        let value_len = entry.get_u16() as usize;
        let value_offset_begin = offset + SIZEOF_U16 + SIZEOF_U16 + key_len + SIZEOF_U16;
        let value_offset_end = value_offset_begin + value_len;
        self.value_range = (value_offset_begin, value_offset_end);
        entry.advance(value_len);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut low = 0;
        let mut high = self.block.offsets.len();
        while low < high {
            let mid = low + (high - low) / 2;
            self.seek_to(mid);
            assert!(self.is_valid());
            match self.key().cmp(&key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => return,
            }
        }
        self.seek_to(low);
    }
}
