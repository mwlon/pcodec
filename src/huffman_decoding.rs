use std::mem;
use std::mem::MaybeUninit;

use crate::BitReader;
use crate::constants::{PREFIX_TABLE_SIZE, PREFIX_TABLE_SIZE_LOG};
use crate::prefix::{Prefix, PrefixDecompressionInfo};
use crate::types::{NumberLike, UnsignedLike};
use crate::errors::{QCompressResult, QCompressError};

#[derive(Clone, Debug)]
pub enum HuffmanTable<Diff: UnsignedLike> {
  Leaf(PrefixDecompressionInfo<Diff>),
  NonLeaf(Box<[HuffmanTable<Diff>; PREFIX_TABLE_SIZE]>),
}

impl<Diff: UnsignedLike> Default for HuffmanTable<Diff> {
  fn default() -> Self {
    HuffmanTable::Leaf(PrefixDecompressionInfo::default())
  }
}

impl<Diff: UnsignedLike> HuffmanTable<Diff> {
  pub fn search_with_reader(&self, reader: &mut BitReader) -> QCompressResult<PrefixDecompressionInfo<Diff>> {
    let mut node = self;
    let mut read_depth = 0;
    loop {
      match node {
        HuffmanTable::Leaf(decompression_info) => {
          reader.rewind(read_depth - decompression_info.depth);
          return Ok(*decompression_info);
        },
        HuffmanTable::NonLeaf(children) => {
          let (bits_read, idx) = reader.read_prefix_table_idx()?;
          read_depth += bits_read;
          node = &children[idx];
          if bits_read != PREFIX_TABLE_SIZE_LOG {
            return match node {
              HuffmanTable::Leaf(decompression_info)
              if decompression_info.depth == read_depth => Ok(*decompression_info),
              HuffmanTable::Leaf(_) => Err(QCompressError::insufficient_data(
                "search_with_reader(): ran out of data parsing Huffman prefix (reached leaf)"
              )),
              HuffmanTable::NonLeaf(_) => Err(QCompressError::insufficient_data(
                "search_with_reader(): ran out of data parsing Huffman prefix (reached parent)"
              )),
            }
          }
        },
      }
    }
  }

  pub fn unchecked_search_with_reader(&self, reader: &mut BitReader) -> PrefixDecompressionInfo<Diff> {
    let mut node = self;
    let mut read_depth = 0;
    loop {
      match node {
        HuffmanTable::Leaf(decompression_info) => {
          reader.rewind(read_depth - decompression_info.depth);
          return *decompression_info;
        },
        HuffmanTable::NonLeaf(children) => {
          node = &children[reader.unchecked_read_prefix_table_idx()];
          read_depth += PREFIX_TABLE_SIZE_LOG;
        },
      }
    }
  }
}

impl<T: NumberLike> From<&Vec<Prefix<T>>> for HuffmanTable<T::Unsigned> {
  fn from(prefixes: &Vec<Prefix<T>>) -> Self {
    if prefixes.is_empty() {
      HuffmanTable::default()
    } else {
      build_from_prefixes_recursive(prefixes, 0)
    }
  }
}

fn build_from_prefixes_recursive<T>(prefixes: &[Prefix<T>], depth: usize) -> HuffmanTable<T::Unsigned>
where T: NumberLike {
  if prefixes.len() == 1 {
    let prefix = &prefixes[0];
    HuffmanTable::Leaf(PrefixDecompressionInfo::from(prefix))
  } else {
    let mut data: [MaybeUninit<HuffmanTable<T::Unsigned>>; PREFIX_TABLE_SIZE] = unsafe {
      MaybeUninit::uninit().assume_init()
    };
    for (idx, uninit_box) in data.iter_mut().enumerate() {
      let mut sub_bits = Vec::new();
      for depth_incr in 0..PREFIX_TABLE_SIZE_LOG {
        sub_bits.push((idx >> (PREFIX_TABLE_SIZE_LOG - 1 - depth_incr)) & 1 > 0);
      }
      let possible_prefixes = prefixes.iter()
        .filter(|&p| {
          for (depth_incr, bit) in sub_bits.iter().enumerate() {
            let total_depth = depth + depth_incr;
            if p.val.len() > total_depth && p.val[total_depth] != *bit {
              return false;
            }
          }
          true
        })
        .cloned()
        .collect::<Vec<Prefix<T>>>();
      let child = build_from_prefixes_recursive(
        &possible_prefixes,
        depth + PREFIX_TABLE_SIZE_LOG,
      );
      uninit_box.write(child);
    }
    let children = unsafe {
      mem::transmute_copy::<_, [HuffmanTable<T::Unsigned>; PREFIX_TABLE_SIZE]>(&data)
    };
    HuffmanTable::NonLeaf(Box::new(children))
  }
}
