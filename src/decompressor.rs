use std::cmp::{max, min};
use std::fmt;
use std::fmt::Debug;

use crate::bit_reader::BitReader;
use crate::bits;
use crate::constants::*;
use crate::errors::QCompressError;
use crate::prefix::{Prefix, PrefixDecompressionInfo};
use crate::types::{NumberLike, UnsignedLike};
use crate::utils;
use std::collections::HashMap;
use std::slice::SliceIndex;

#[derive(Clone, Debug)]
pub enum PrefixTrie<T>
where
  T: NumberLike,
{
  Children(*const PrefixTrie<T>, *const PrefixTrie<T>), // vector indices for the left and right nodes
  Value(PrefixDecompressionInfo<T::Unsigned>),

  // pub child0: Option<&'a PrefixTrie<'a, T>>,
  // pub child1: Option<&'a PrefixTrie<'a, T>>,
  // pub is_leaf: bool,
  // pub value: PrefixDecompressionInfo<T::Unsigned>,
}

impl<T> PrefixTrie<T>
where
  T: NumberLike,
{
  pub fn from_prefixes(
    max_depth: u32,
    prefix_binary_map: HashMap<Vec<bool>, PrefixDecompressionInfo<T::Unsigned>>,
    prefix_trie_nodes: &mut Vec<PrefixTrie<T>>,
  ) -> Result<PrefixTrie<T>, QCompressError> {
    if prefix_binary_map.is_empty() {
      return Ok(PrefixTrie::Value(PrefixDecompressionInfo {
        lower_unsigned: T::Unsigned::ZERO,
        range: T::Unsigned::MAX,
        k: T::PHYSICAL_BITS as u32,
        run_len_jumpstart: None,
      }));
    }

    Self::from_prefix_tail(
      0,
      Vec::new(),
      max_depth,
      &prefix_binary_map,
      prefix_trie_nodes,
    )
  }

  pub fn from_prefix_tail(
    current_depth: u32,
    current_val: Vec<bool>,
    max_depth: u32,
    prefix_binary_map: &HashMap<Vec<bool>, PrefixDecompressionInfo<T::Unsigned>>,
    prefix_trie_nodes: &mut Vec<PrefixTrie<T>>,
  ) -> Result<PrefixTrie<T>, QCompressError> {
    if current_depth > max_depth {
      return Err(QCompressError::PrefixesError {binary_string: current_val});
    }

    if let Some(info) = prefix_binary_map.get(&current_val) {
      let leaf = PrefixTrie::Value(*info);
      Ok(leaf)
    } else {
      let mut left_val = current_val.clone();
      left_val.push(false);
      let mut right_val = current_val.clone();
      right_val.push(true);
      let left = Self::from_prefix_tail(current_depth + 1, left_val, max_depth, prefix_binary_map, prefix_trie_nodes)?;
      let right = Self::from_prefix_tail(current_depth + 1, right_val, max_depth, prefix_binary_map, prefix_trie_nodes)?;
      let left_idx = prefix_trie_nodes.len();
      prefix_trie_nodes.push(left);
      prefix_trie_nodes.push(right);
      // we rely on prefix_trie_nodes not resizing, having been given enough capacity to begin with
      Ok(PrefixTrie::Children(
        &prefix_trie_nodes[left_idx] as *const PrefixTrie<T>,
        &prefix_trie_nodes[left_idx + 1] as *const PrefixTrie<T>
      ))
    }
  }
}

#[derive(Clone)]
pub struct Decompressor<T>
where
  T: NumberLike,
{
  prefixes: Vec<Prefix<T>>,
  prefix_trie_nodes: Vec<PrefixTrie<T>>,
  prefix_trie_root: PrefixTrie<T>,
  max_depth: u32,
  n: usize,
  is_single_prefix: bool,
}

impl<T> Decompressor<T>
where
  T: NumberLike,
{
  pub fn new(prefixes: Vec<Prefix<T>>, n: usize) -> Result<Self, QCompressError> {
    let mut max_depth = 0;
    let n_pref = prefixes.len();
    let mut prefix_binary_map = HashMap::<Vec<bool>, PrefixDecompressionInfo<T::Unsigned>>::with_capacity(n_pref);
    for p in &prefixes {
      prefix_binary_map.insert(p.val.clone(), p.into());
      max_depth = max(max_depth, p.val.len() as u32);
    }
    let mut prefix_trie_nodes = Vec::with_capacity(2 * n_pref);
    let prefix_trie_root = PrefixTrie::from_prefixes(
      max_depth,
      prefix_binary_map,
      &mut prefix_trie_nodes
    )?;

    let is_single_prefix = prefixes.len() == 1;
    Ok(Decompressor {
      prefixes,
      prefix_trie_nodes,
      prefix_trie_root,
      max_depth,
      n,
      is_single_prefix,
    })
  }

  pub fn from_reader(bit_reader: &mut BitReader) -> Result<Self, QCompressError> {
    let bytes = bit_reader.read_bytes(MAGIC_HEADER.len())?;
    if bytes != MAGIC_HEADER {
      return Err(QCompressError::MagicHeaderError {
        header: bytes.to_vec(),
      });
    }
    let bytes = bit_reader.read_bytes(1)?;
    let byte = bytes[0];
    if byte != T::HEADER_BYTE {
      return Err(QCompressError::HeaderDtypeError {
        header_byte: byte,
        decompressor_byte: T::HEADER_BYTE,
      });
    }

    let n = bit_reader.read_usize(BITS_TO_ENCODE_N_ENTRIES as usize);
    let n_pref = bit_reader.read_usize(MAX_MAX_DEPTH as usize);
    let mut prefixes = Vec::with_capacity(n_pref);
    for _ in 0..n_pref {
      let lower_bits = bit_reader.read(T::PHYSICAL_BITS);
      let lower = T::from_bytes(bits::bits_to_bytes(lower_bits));
      let upper_bits = bit_reader.read(T::PHYSICAL_BITS);
      let upper = T::from_bytes(bits::bits_to_bytes(upper_bits));
      let code_len = bit_reader.read_usize(BITS_TO_ENCODE_PREFIX_LEN as usize);
      let val = bit_reader.read(code_len);
      let jumpstart = if bit_reader.read_one() {
        Some(bit_reader.read_usize(BITS_TO_ENCODE_JUMPSTART as usize))
      } else {
        None
      };
      prefixes.push(Prefix::new(val, lower, upper, jumpstart));
    }

    Decompressor::new(prefixes, n)
  }

  pub fn decompress(&self, reader: &mut BitReader) -> Vec<T> {
    self.decompress_n(reader, self.n)
  }

  fn next_prefix(&self, reader: &mut BitReader) -> PrefixDecompressionInfo<T::Unsigned> {
    let mut prefix_trie = &self.prefix_trie_root as *const PrefixTrie<T>;
    unsafe {
      while let PrefixTrie::Children(left, right) = *prefix_trie {
        prefix_trie = if reader.read_one() {
          right
        } else {
          left
        }
      }
      match *prefix_trie {
        PrefixTrie::Value(res) => res,
        _ => panic!("unreachable")
      }
    }
  }

  pub fn decompress_n(&self, reader: &mut BitReader, n: usize) -> Vec<T> {
    let mut res = Vec::with_capacity(n);
    let mut i = 0;
    while i < n {
      let p = self.next_prefix(reader);

      let reps = match p.run_len_jumpstart {
        None => 1,
        Some(jumpstart) => {
          // we stored the number of occurrences minus 1
          // because we knew it's at least 1
          min(reader.read_varint(jumpstart) + 1, n - i)
        }
      };

      for _ in 0..reps {
        let mut offset = reader.read_diff(p.k as usize);
        if p.k < T::Unsigned::BITS {
          let most_significant = T::Unsigned::ONE << p.k;
          if p.range - offset >= most_significant && reader.read_one() {
            offset |= most_significant;
          }
        }
        res.push(T::from_unsigned(p.lower_unsigned + offset));
      }
      i += reps;
    }
    res
  }
}

impl<T> Debug for Decompressor<T>
where
  T: NumberLike,
{
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    utils::display_prefixes(&self.prefixes, f)
  }
}
