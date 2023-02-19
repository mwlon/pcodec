use std::cmp::min;

use crate::bit_reader::BitReader;
use crate::constants::MAX_PREFIX_TABLE_SIZE_LOG;
use crate::data_types::{NumberLike, UnsignedLike};
use crate::errors::{QCompressError, QCompressResult};
use crate::prefix::{Prefix, PrefixDecompressionInfo};

#[derive(Clone, Debug)]
pub enum HuffmanTable<U: UnsignedLike> {
  Leaf(PrefixDecompressionInfo<U>),
  NonLeaf {
    table_size_log: usize,
    children: Vec<HuffmanTable<U>>,
  },
}

impl<U: UnsignedLike> Default for HuffmanTable<U> {
  fn default() -> Self {
    HuffmanTable::Leaf(PrefixDecompressionInfo::default())
  }
}

impl<U: UnsignedLike> HuffmanTable<U> {
  pub fn search_with_reader(
    &self,
    reader: &mut BitReader,
  ) -> QCompressResult<PrefixDecompressionInfo<U>> {
    let mut node = self;
    let mut read_depth = 0;
    loop {
      match node {
        HuffmanTable::Leaf(decompression_info) => {
          reader.rewind_prefix_overshoot(read_depth - decompression_info.depth);
          return Ok(*decompression_info);
        }
        HuffmanTable::NonLeaf {
          table_size_log,
          children,
        } => {
          let (bits_read, idx) = reader.read_prefix_table_idx(*table_size_log)?;
          read_depth += bits_read;
          node = &children[idx];
          if bits_read != *table_size_log {
            return match node {
              HuffmanTable::Leaf(decompression_info) if decompression_info.depth == read_depth => {
                Ok(*decompression_info)
              }
              HuffmanTable::Leaf(_) => Err(QCompressError::insufficient_data(
                "search_with_reader(): ran out of data parsing Huffman prefix (reached leaf)",
              )),
              HuffmanTable::NonLeaf {
                table_size_log: _,
                children: _,
              } => Err(QCompressError::insufficient_data(
                "search_with_reader(): ran out of data parsing Huffman prefix (reached parent)",
              )),
            };
          }
        }
      }
    }
  }

  pub fn unchecked_search_with_reader(&self, reader: &mut BitReader) -> PrefixDecompressionInfo<U> {
    let mut node = self;
    let mut read_depth = 0;
    loop {
      match node {
        HuffmanTable::Leaf(decompression_info) => {
          reader.rewind_prefix_overshoot(read_depth - decompression_info.depth);
          return *decompression_info;
        }
        HuffmanTable::NonLeaf {
          table_size_log,
          children,
        } => {
          let idx = reader.unchecked_read_usize(*table_size_log);
          node = &children[idx];
          read_depth += table_size_log;
        }
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

fn build_from_prefixes_recursive<T: NumberLike>(
  prefixes: &[Prefix<T>],
  depth: usize,
) -> HuffmanTable<T::Unsigned> {
  if prefixes.len() == 1 {
    let prefix = &prefixes[0];
    HuffmanTable::Leaf(PrefixDecompressionInfo::from(prefix))
  } else {
    let max_depth = prefixes.iter().map(|p| p.code.len()).max().unwrap();
    let table_size_log: usize = min(MAX_PREFIX_TABLE_SIZE_LOG, max_depth - depth);
    let table_size = 1 << table_size_log;

    let mut children = Vec::new();
    for idx in 0..table_size {
      let mut sub_bits = Vec::new();
      for depth_incr in 0..table_size_log {
        sub_bits.push((idx >> depth_incr) & 1 > 0);
      }
      let possible_prefixes = prefixes
        .iter()
        .filter(|&p| {
          for (depth_incr, bit) in sub_bits.iter().enumerate() {
            let total_depth = depth + depth_incr;
            if p.code.len() > total_depth && p.code[total_depth] != *bit {
              return false;
            }
          }
          true
        })
        .cloned()
        .collect::<Vec<Prefix<T>>>();
      let child = build_from_prefixes_recursive(&possible_prefixes, depth + table_size_log);
      children.push(child);
    }
    HuffmanTable::NonLeaf {
      table_size_log,
      children,
    }
  }
}
