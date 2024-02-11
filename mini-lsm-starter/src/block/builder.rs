#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod
use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::{Block, SIZEOF_U16};

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}
fn compute_overlap(first_key: KeySlice, key: KeySlice) -> usize {
    let mut i = 0;
    loop {
        if i >= first_key.len() || i >= key.len() {
            break;
        }
        if first_key.raw_ref()[i] != key.raw_ref()[i] {
            break;
        }
        i += 1;
    }
    i
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }
    fn estimated_size(&self) -> usize {
        SIZEOF_U16 /* number of key-value pairs in the block */ +  self.offsets.len() * SIZEOF_U16 /* offsets */ + self.data.len()
        // key-value pairs
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    ///
    /// 功能和流程
    // #[must_use]属性：这个属性意味着调用这个函数的返回值必须被使用。这是Rust的一种约定，用于强调函数的结果不应该被忽略，因为它包含了重要的信息（在这个例子中，是操作是否成功）。

    // 参数：

    // &mut self：这个方法取一个可变引用到自身，表示它会修改调用它的实例。
    // key: KeySlice：要添加的键，使用了之前定义的KeySlice类型。
    // value: &[u8]：与键关联的值，它是一个字节切片。
    // 空键检查：使用assert!宏来确保传入的键不是空的。

    // 大小检查：如果添加当前键值对后预估的总大小超过了self.block_size的限制，并且数据结构不为空，则方法返回false，表示添加失败。这里还额外计算了键和值的长度，以及额外的SIZEOF_U16 * 3，这可能是为了存储键长度、值长度和某个偏移量。

    // 添加偏移量：将当前数据的偏移量（即self.data.len()）添加到self.offsets数组中。这个偏移量是用来标记新添加的数据在self.data中的起始位置。

    // 计算键的重叠：使用compute_overlap函数计算新键与第一个键的重叠长度。

    // 编码键和值：

    // 首先编码键的重叠长度和实际添加的键的长度（减去重叠部分）。
    // 接着，将键（除去重叠部分）和值的内容添加到self.data中。
    // 对于键和值的长度，使用put_u16方法编码，这意味着这些长度值被存储为u16类型。
    // 更新第一个键：如果self.first_key是空的（即这是第一个被添加的键值对），则更新self.first_key为当前键。

    // 返回值：如果方法能执行到最后，返回true，表示添加操作成功。

    // 总结
    // 这个add方法提供了一个精细的控制机制，用于在保持数据结构内部一致性和遵循大小限制的同时，向数据结构中添加新的键值对。它通过编码键和值的长度及内容，以及处理键之间的重叠部分，确保了数据的高效存储。此外，它还通过预估大小来避免超出预设的存储限制，确保数据结构不会无限制地增长。这种方法在设计用于存储键值对的自定义数据结构（如数据库的索引块）时非常有用。
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");
        if self.estimated_size() + key.len() + value.len() + SIZEOF_U16 * 3 /* key_len, value_len and offset */ > self.block_size
            && !self.is_empty()
        {
            return false;
        }
        // Add the offset of the data into the offset array.
        self.offsets.push(self.data.len() as u16);
        let overlap = compute_overlap(self.first_key.as_key_slice(), key);
        // Encode key overlap.
        self.data.put_u16(overlap as u16);
        // Encode key length.
        self.data.put_u16((key.len() - overlap) as u16);
        // Encode key content.
        self.data.put(&key.raw_ref()[overlap..]);
        // Encode value length.
        self.data.put_u16(value.len() as u16);
        // Encode value content.
        self.data.put(value);

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("block should not be empty");
        }
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
