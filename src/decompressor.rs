use std::cmp::max;
use std::fmt;
use std::fmt::Display;

use crate::bit_reader::BitReader;
use crate::bits::*;
use crate::int64::*;
use crate::prefix::Prefix;
use crate::utils;

pub struct I64Decompressor {
  prefixes: Vec<Prefix>,
  prefix_map: Vec<Option<Prefix>>,
  prefix_len_map: Vec<u32>,
  max_depth: u32,
  n: usize,
}

impl I64Decompressor {
  pub fn new(prefixes: Vec<Prefix>, n: usize) -> I64Decompressor {
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
      let i = bits_to_usize_truncated(&p.val, max_depth);
      prefix_map[i] = Some(p.clone());
      prefix_len_map[i] = p.val.len() as u32;
    }

    I64Decompressor {
      prefixes,
      prefix_map,
      prefix_len_map,
      max_depth,
      n,
    }
  }

  pub fn from_reader(bit_reader: &mut BitReader) -> I64Decompressor {
    let n = bits_to_usize(bit_reader.read(BITS_TO_ENCODE_N_ENTRIES as usize));
    let n_pref = bits_to_usize(bit_reader.read(MAX_MAX_DEPTH as usize));
    let mut prefixes = Vec::with_capacity(n_pref);
    for _ in 0..n_pref {
      let lower_bits = bit_reader.read(64);
      let lower = bits_to_int64(lower_bits);
      let upper_bits = bit_reader.read(64);
      let upper = bits_to_int64(upper_bits);
      let code_len_bits = bit_reader.read(BITS_TO_ENCODE_PREFIX_LEN as usize);
      let code_len = bits_to_usize(code_len_bits);
      let val = bit_reader.read(code_len);
      prefixes.push(Prefix::new(val, lower, upper));
    }

    let decompressor = I64Decompressor::new(prefixes, n);

    return decompressor;
  }

  pub fn decompress(&self, reader: &mut BitReader) -> Vec<i64> {
    let pow = (1_usize) << self.max_depth;
    let mut res = Vec::with_capacity(self.n);
    // handle the case when there's just one prefix of length 0
    let default_lower;
    let default_upper;
    let default_k;
    match &self.prefix_map[0] {
      Some(p) if p.val.len() == 0 => {
        default_lower = p.lower;
        default_upper = p.upper;
        default_k = p.k;
      },
      _ => {
        default_lower = 0;
        default_upper = 0;
        default_k = 0;
      }
    };
    for _ in 0..self.n {
      let mut lower = default_lower;
      let mut upper = default_upper;
      let mut k = default_k;
      let mut prefix_idx = 0;
      let mut m = pow;
      for prefix_len in 1..self.max_depth + 1 {
        m >>= 1;
        if reader.read_one() {
          prefix_idx |= m;
        }
        if self.prefix_len_map[prefix_idx] == prefix_len {
          let p = self.prefix_map[prefix_idx].as_ref().unwrap();
          lower = p.lower;
          upper = p.upper;
          k = p.k;
          break;
        }
      }
      let range = utils::u64_diff(upper, lower);
      let mut offset = reader.read_u64(k as usize);
      if k < 64 {
        let most_significant = 1_u64 << k;
        if range - offset >= most_significant {
          if reader.read_one() {
            offset += most_significant;
          }
        }
      }
      res.push(utils::i64_plus_u64(lower, offset));
    }
    res
  }
}

fn display_prefixes(prefixes: &Vec<Prefix>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
  let s = prefixes
    .iter()
    .map(|p| format!(
      "\t{}: {} to {} (density {})",
      bits_to_string(&p.val),
      p.lower,
      p.upper,
      2.0_f64.powf(-(p.val.len() as f64)) / (p.upper as f64 - p.lower as f64)
    ))
    .collect::<Vec<String>>()
    .join("\n");
  write!(f, "{}", s)
}

impl Display for I64Decompressor {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    display_prefixes(&self.prefixes, f)
  }
}

