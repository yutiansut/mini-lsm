#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;
pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

// encode方法
// encode方法的作用是将Block实例的内容编码成一个连续的字节序列。它首先复制data字段的内容，然后追加每个偏移量，并在最后添加偏移量的总数。这个方法确保了编码后的数据包含了所有必要的信息，以便可以从这些字节序列中准确地重建原始Block实例。

// decode方法
// decode方法执行相反的操作：它从提供的字节序列中解析出偏移量和数据，构建并返回一个新的Block实例。具体步骤如下：

// 解析偏移量数量：首先从字节序列的末尾读取表示偏移量数量的u16值。这需要注意SIZEOF_U16（通常为2）来正确定位这个值的起始位置。

// 计算数据区域的结束位置：通过从总字节长度中减去偏移量部分和偏移量数量所占的字节，计算出实际数据部分的结束位置。

// 解析偏移量数组：然后，从计算出的数据结束位置到除去表示偏移量数量的最后2个字节的区域，读取偏移量数组。每个偏移量占用SIZEOF_U16字节，通过分块（chunks(SIZEOF_U16)）和映射（map(|x| x.get_u16())）操作来解析每个偏移量。

// 提取数据部分：数据部分位于字节序列的开始到计算出的数据结束位置之间。这部分数据被复制到新的Vec<u8>中。

// 构建Block实例：使用解析出的数据和偏移量数组构建并返回一个新的Block实例。
impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf = self.data.clone();
        let offsets_len = self.offsets.len();
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        // Adds number of elements at the end of the block
        buf.put_u16(offsets_len as u16);
        buf.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`

    pub fn decode(data: &[u8]) -> Self {
        // get number of elements in the block
        let entry_offsets_len = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
        let data_end = data.len() - SIZEOF_U16 - entry_offsets_len * SIZEOF_U16;
        let offsets_raw = &data[data_end..data.len() - SIZEOF_U16];
        // get offset array
        let offsets = offsets_raw
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect();
        // retrieve data
        let data = data[0..data_end].to_vec();
        Self { data, offsets }
    }
}
