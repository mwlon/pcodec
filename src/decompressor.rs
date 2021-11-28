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

#[derive(Clone)]
pub struct PrefixTrie<T> where T: NumberLike {
  pub child0: Option<Box<PrefixTrie<T>>>,
  pub child1: Option<Box<PrefixTrie<T>>>,
  pub value: Option<PrefixDecompressionInfo<T::Unsigned>>,
}

impl<T> Default for PrefixTrie<T> where T: NumberLike {
  fn default() -> Self {
    PrefixTrie::new()
  }
}

impl<T> PrefixTrie<T> where T: NumberLike {
  pub fn new() -> PrefixTrie<T> {
    PrefixTrie {
      child0: None,
      child1: None,
      value: None,
    }
  }

  pub fn insert(&mut self, key: &[bool], value: &Prefix<T>) {
    if key.is_empty(){
      self.value = Some(value.into());
    } else {
      let (head, tail) = key.split_at(1);
      if head[0] {
        if self.child1.is_none() {
          self.child1 = Some(Box::new(PrefixTrie::new()));
        }
        self.child1.as_mut().unwrap().insert(tail, value);
      } else {
        if self.child0.is_none() {
          self.child0 = Some(Box::new(PrefixTrie::new()));
        }
        self.child0.as_mut().unwrap().insert(tail, value);
      }
    }
  }

  pub fn contains_key(&self, key: &bool) -> bool {
    if *key {
      self.child1.is_some()
    } else {
      self.child0.is_some()
    }
  }

  pub fn is_leaf(&self) -> bool {
    self.child0.is_none() && self.child1.is_none()
  }

  pub fn select_child(&self, key: &bool) -> &PrefixTrie<T> {
    if *key {
      self.child1.as_ref().unwrap()
    } else {
      self.child0.as_ref().unwrap()
    }
  }
}

#[derive(Clone)]
pub struct Decompressor<T> where T: NumberLike {
  prefixes: Vec<Prefix<T>>,
  prefix_map: Vec<PrefixDecompressionInfo<T::Unsigned>>,
  prefix_len_map: Vec<u32>,
  prefix_trie: PrefixTrie<T>,
  max_depth: u32,
  n: usize,
  is_single_prefix: bool,
}

impl<T> Decompressor<T> where T: NumberLike {
  pub fn new(prefixes: Vec<Prefix<T>>, n: usize) -> Self {
    let mut max_depth = 0;
    for p in &prefixes {
      max_depth = max(max_depth, p.val.len() as u32);
    }
    let n_pref = 1_usize << max_depth;
    let mut prefix_map = Vec::with_capacity(n_pref);
    let mut prefix_len_map = Vec::with_capacity(n_pref);
    for _ in 0..n_pref {
      prefix_map.push(PrefixDecompressionInfo::new());
      prefix_len_map.push(u32::MAX);
    }
    for p in &prefixes {
      let i = bits::bits_to_usize_truncated(&p.val, max_depth);
      prefix_map[i] = p.into();
      prefix_len_map[i] = p.val.len() as u32;
    }

    let mut prefix_trie = PrefixTrie::new();
    for p in &prefixes {
      let vals = &p.val;
      prefix_trie.insert(vals, p);
    }

    let is_single_prefix = prefixes.len() == 1;
    Decompressor {
      prefixes,
      prefix_map,
      prefix_len_map,
      prefix_trie,
      max_depth,
      n,
      is_single_prefix,
    }
  }

  pub fn from_reader(bit_reader: &mut BitReader) -> Result<Self, QCompressError> {
    let bytes = bit_reader.read_bytes(MAGIC_HEADER.len())?;
    if bytes != MAGIC_HEADER {
      return Err(QCompressError::MagicHeaderError {
        header: bytes.to_vec()
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

    let decompressor = Decompressor::new(prefixes, n);

    Ok(decompressor)
  }

  pub fn decompress(&self, reader: &mut BitReader) -> Vec<T> {
    self.decompress_n(reader, self.n)
  }

  fn next_prefix(&self, reader: &mut BitReader) -> PrefixDecompressionInfo<T::Unsigned> {
      let mut prefix_trie = &self.prefix_trie;
      while !prefix_trie.is_leaf() {
        let val = reader.read_one();
        prefix_trie = prefix_trie.select_child(&val);
      }
      prefix_trie.value.unwrap()
  }

  pub fn decompress_n(&self, reader: &mut BitReader, n: usize) -> Vec<T> {
    let mut res = Vec::with_capacity(n);
    let mut i = 0;
    while i < n {
      let p = self.next_prefix(reader);

      let reps = match p.run_len_jumpstart {
        None => {
          1
        },
        Some(jumpstart) => {
          // we stored the number of occurrences minus 1
          // because we knew it's at least 1
          min(reader.read_varint(jumpstart) + 1, n - i)
        },
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

impl<T> Debug for Decompressor<T> where T: NumberLike {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    utils::display_prefixes(&self.prefixes, f)
  }
}

