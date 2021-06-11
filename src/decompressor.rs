use std::cmp::max;
use std::fmt;
use std::fmt::Debug;
use std::marker::PhantomData;

use crate::bit_reader::BitReader;
use crate::bits;
use crate::prefix::Prefix;
use crate::types::{DataType, NumberLike};
use crate::utils;
use crate::constants::*;
use std::error::Error;
use crate::errors::{MagicHeaderError, HeaderDtypeError};

#[derive(Clone)]
pub struct Decompressor<T, DT> where T: NumberLike, DT: DataType<T> {
  prefixes: Vec<Prefix<T>>,
  prefix_map: Vec<Option<Prefix<T>>>,
  prefix_len_map: Vec<u32>,
  max_depth: u32,
  n: usize,
  data_type: PhantomData<DT>,
}

impl<T, DT> Decompressor<T, DT> where T: NumberLike, DT: DataType<T> {
  pub fn new(prefixes: Vec<Prefix<T>>, n: usize) -> Self {
    let mut max_depth = 0;
    for p in &prefixes {
      max_depth = max(max_depth, p.val.len() as u32);
    }
    let n_pref = 1_usize << max_depth;
    let mut prefix_map = Vec::with_capacity(n_pref);
    let mut prefix_len_map = Vec::with_capacity(n_pref);
    for _ in 0..n_pref {
      prefix_map.push(None);
      prefix_len_map.push(u32::MAX);
    }
    for p in &prefixes {
      let i = bits::bits_to_usize_truncated(&p.val, max_depth);
      prefix_map[i] = Some(p.clone());
      prefix_len_map[i] = p.val.len() as u32;
    }

    Decompressor {
      prefixes,
      prefix_map,
      prefix_len_map,
      max_depth,
      n,
      data_type: PhantomData,
    }
  }

  pub fn from_reader(bit_reader: &mut BitReader) -> Result<Self, Box<dyn Error>> {
    let bytes = bit_reader.read_bytes(MAGIC_HEADER.len())?;
    if bytes != MAGIC_HEADER {
      return Err(Box::new(MagicHeaderError {
        header: bytes.to_vec()
      }));
    }
    let bytes = bit_reader.read_bytes(1)?;
    let byte = bytes[0];
    if byte != DT::HEADER_BYTE {
      return Err(Box::new(HeaderDtypeError {
        dtype_byte: byte,
        expected_byte: DT::HEADER_BYTE,
      }));
    }

    let n = bit_reader.read_u64(BITS_TO_ENCODE_N_ENTRIES as usize) as usize;
    let n_pref = bit_reader.read_u64(MAX_MAX_DEPTH as usize) as usize;
    let mut prefixes = Vec::with_capacity(n_pref);
    for _ in 0..n_pref {
      let lower_bits = bit_reader.read(DT::BIT_SIZE);
      let lower = DT::from_bytes(bits::bits_to_bytes(lower_bits));
      let upper_bits = bit_reader.read(DT::BIT_SIZE);
      let upper = DT::from_bytes(bits::bits_to_bytes(upper_bits));
      let code_len = bit_reader.read_u64(BITS_TO_ENCODE_PREFIX_LEN as usize) as usize;
      let val = bit_reader.read(code_len);
      let jumpstart = if bit_reader.read_one() {
        Some(bit_reader.read_u64(BITS_TO_ENCODE_JUMPSTART as usize) as usize)
      } else {
        None
      };
      prefixes.push(Prefix::new(val, lower, upper, DT::offset_diff(upper, lower), jumpstart));
    }

    let decompressor = Decompressor::new(prefixes, n);

    Ok(decompressor)
  }

  pub fn decompress(&self, reader: &mut BitReader) -> Vec<T> {
    self.decompress_n(reader, self.n)
  }

  pub fn decompress_n(&self, reader: &mut BitReader, n: usize) -> Vec<T> {
    let pow = 1_usize << self.max_depth;
    let mut res = Vec::with_capacity(n);
    // handle the case when there's just one prefix of length 0
    let default_lower;
    let default_upper;
    let default_k;
    let default_jumpstart;
    match &self.prefix_map[0] {
      Some(p) if p.val.is_empty() => {
        default_lower = p.lower;
        default_upper = p.upper;
        default_k = p.k;
        default_jumpstart = p.run_len_jumpstart;
      },
      _ => {
        // any usage of these should be unreachable
        default_lower = DT::ZERO;
        default_upper = DT::ZERO;
        default_k = 0;
        default_jumpstart = None;
      }
    };
    let mut i = 0;
    while i < n {
      let mut lower = default_lower;
      let mut upper = default_upper;
      let mut k = default_k;
      let mut run_len_jumpstart = default_jumpstart;
      let mut prefix_idx = 0;
      for prefix_len in 1..self.max_depth + 1 {
        if reader.read_one() {
          prefix_idx |= pow >> prefix_len;
        }
        if self.prefix_len_map[prefix_idx] == prefix_len {
          let p = self.prefix_map[prefix_idx].as_ref().unwrap();
          lower = p.lower;
          upper = p.upper;
          k = p.k;
          run_len_jumpstart = p.run_len_jumpstart;
          break;
        }
      }
      let range = DT::offset_diff(upper, lower);

      let reps = match run_len_jumpstart {
        None => {
          1
        },
        Some(jumpstart) => {
          // we stored the number of occurrences minus 1
          // because we knew it's at least 1
          reader.read_varint(jumpstart) + 1
        },
      };

      for _ in 0..reps {
        let mut offset = reader.read_u64(k as usize);
        if k < 64 {
          let most_significant = 1_u64 << k;
          if range - offset >= most_significant && reader.read_one() {
            offset |= most_significant;
          }
        }
        res.push(DT::add_offset(lower, offset));
      }
      i += reps;
    }
    res
  }
}

impl<T, DT> Debug for Decompressor<T, DT> where T: NumberLike, DT: DataType<T> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    utils::display_prefixes(&self.prefixes, f)
  }
}

