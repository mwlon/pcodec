use crate::BitReader;
use crate::constants::{PREFIX_TABLE_SIZE, PREFIX_TABLE_SIZE_LOG};
use crate::prefix::{PrefixDecompressionInfo, Prefix};
use crate::types::{NumberLike, UnsignedLike};
use std::mem;
use std::mem::MaybeUninit;

#[derive(Clone, Debug)]
pub enum HuffmanTable<U: UnsignedLike> {
  Leaf(PrefixDecompressionInfo<U>),
  NonLeaf([Box<HuffmanTable<U>>; PREFIX_TABLE_SIZE]),
}

impl<U: UnsignedLike> HuffmanTable<U> {
  pub fn search_with_reader(&self, reader: &mut BitReader) -> PrefixDecompressionInfo<U> {
    let mut node = self;
    let mut read_depth = 0;
    loop {
      match node {
        HuffmanTable::Leaf(decompression_info) => {
          reader.rewind(read_depth - decompression_info.depth);
          return *decompression_info;
        },
        HuffmanTable::NonLeaf(children) => {
          let (bits_read, idx) = reader.read_prefix_table_idx();
          read_depth += bits_read;
          node = &children[idx];
        },
      }
    }
  }
}

impl<T: NumberLike> From<Vec<Prefix<T>>> for HuffmanTable<T::Unsigned> {
  fn from(prefixes: Vec<Prefix<T>>) -> Self {
    build_from_prefixes_recursive(prefixes, 0)
  }
}

fn build_from_prefixes_recursive<T>(prefixes: Vec<Prefix<T>>, depth: usize) -> HuffmanTable<T::Unsigned>
where T: NumberLike {
  // there is an unreachable case we can ignore where prefixes.is_empty()
  if prefixes.len() == 1 {
    let prefix = &prefixes[0];
    HuffmanTable::Leaf(PrefixDecompressionInfo::from(prefix))
  } else {
    let mut data: [MaybeUninit<Box<HuffmanTable<T::Unsigned>>>; PREFIX_TABLE_SIZE] = unsafe {
      MaybeUninit::uninit().assume_init()
    };
    for idx in 0..PREFIX_TABLE_SIZE {
      let mut sub_bits = Vec::new();
      for depth_incr in 0..PREFIX_TABLE_SIZE_LOG {
        sub_bits.push((idx >> (PREFIX_TABLE_SIZE_LOG - 1 - depth_incr)) & 1 > 0);
      }
      let possible_prefixes = prefixes.iter()
        .filter(|&p| {
          for depth_incr in 0..PREFIX_TABLE_SIZE_LOG {
            let total_depth = depth + depth_incr;
            if p.val.len() > total_depth && p.val[total_depth] != sub_bits[depth_incr] {
              return false;
            }
          }
          true
        })
        .cloned()
        .collect::<Vec<Prefix<T>>>();
      let child = build_from_prefixes_recursive(
        possible_prefixes,
        depth + PREFIX_TABLE_SIZE_LOG,
      );
      data[idx].write(Box::new(child));
    }
    let children = unsafe {
      mem::transmute::<_, [Box<HuffmanTable<T::Unsigned>>; PREFIX_TABLE_SIZE]>(data)
    };
    HuffmanTable::NonLeaf(children)
  }
}
