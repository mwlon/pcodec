use std::cmp::max;
use std::fmt;
use std::fmt::Display;

use crate::bit_reader::BitReader;
use crate::bits::*;
use crate::huffman;
use crate::prefix::{Prefix, PrefixIntermediate};

const MAX_MAX_DEPTH: u32 = 15;
const BITS_TO_ENCODE_PREFIX_LEN: u32 = 4; // should be (MAX_MAX_DEPTH + 1).log2().ceil()
const MAX_ENTRIES: u64 = ((1 as u64) << 32) - 1;
const BITS_TO_ENCODE_N_ENTRIES: u32 = 32; // should be (MAX_ENTRIES + 1).log2().ceil()

fn combine_improvement(p0: &PrefixIntermediate, p1: &PrefixIntermediate, n: usize) -> f64 {
  let p0_r_cost = base2_bits(p0.upper, p0.lower);
  let p1_r_cost = base2_bits(p1.upper, p1.lower);
  let combined_r_cost = base2_bits(p1.upper, p0.lower);
  let p0_d_cost = depth_bits(p0.weight, n);
  let p1_d_cost = depth_bits(p1.weight, n);
  let combined_d_cost = depth_bits(p0.weight + p1.weight, n);
  let meta_cost = 136.0;

  let separate_cost = 2.0 * meta_cost +
    (p0_r_cost + p0_d_cost) * p0.weight as f64+
    (p1_r_cost + p1_d_cost) * p1.weight as f64;
  let combined_cost = meta_cost +
    (combined_r_cost + combined_d_cost) * (p0.weight + p1.weight) as f64;
  let bits_saved = separate_cost - combined_cost;
  let improvement = bits_saved / (p0.weight + p1.weight) as f64;
  return improvement;
}

fn push_pref(
  seq: &mut Vec<PrefixIntermediate>,
  bucket_idx: &mut usize,
  i: usize,
  j: usize,
  n_bucket: usize,
  n: usize,
  sorted: &Vec<i64>,
) {
  seq.push(PrefixIntermediate::new((j - i) as u64, sorted[i], sorted[j - 1]));
  *bucket_idx = max(*bucket_idx + 1, (j * n_bucket) / n);
}

pub struct Compressor {
  prefixes: Vec<Prefix>,
  n: usize,
}

pub struct Decompressor {
  prefixes: Vec<Prefix>,
  prefix_map: Vec<Option<Prefix>>,
  prefix_len_map: Vec<u32>,
  max_depth: u32,
  n: usize,
}

impl Compressor {
  pub fn new(prefixes: Vec<Prefix>, n: usize) -> Compressor {
    Compressor {
      prefixes,
      n,
    }
  }

  pub fn train(ints: &Vec<i64>, max_depth: u32) -> Result<Compressor, String> {
    if max_depth > MAX_MAX_DEPTH {
      return Err(format!("max depth cannot exceed {}", MAX_MAX_DEPTH));
    }
    if ints.len() as u64 > MAX_ENTRIES {
      return Err(format!("number of entries cannot exceed {}", MAX_ENTRIES));
    }

    let mut sorted = ints.clone();
    sorted.sort();
    let n = ints.len();
    let n_bucket = (1 as usize) << max_depth;
    let mut prefix_sequence: Vec<PrefixIntermediate> = Vec::new();
    let seq_ptr = &mut prefix_sequence;

    let mut bucket_idx = 0 as usize;
    let bucket_idx_ptr = &mut bucket_idx;

    let mut i = 0;
    let mut backup_j = 0 as usize;
    for j in 0..n {
      let target_j = ((*bucket_idx_ptr + 1) * n) / n_bucket;
      if j > 0 && sorted[j] == sorted[j - 1] {
        if j >= target_j && j - target_j >= target_j - backup_j && backup_j > i {
          push_pref(seq_ptr, bucket_idx_ptr, i, backup_j, n_bucket, n, &sorted);
          i = backup_j;
        }
      } else {
        backup_j = j;
        if j >= target_j {
          push_pref(seq_ptr, bucket_idx_ptr, i, j, n_bucket, n, &sorted);
          i = j;
        }
      }
    }
    push_pref(seq_ptr, bucket_idx_ptr, i, n, n_bucket, n, &sorted);

    let mut can_improve = true;
    while can_improve {
      can_improve = false;
      let mut best_i = -1 as i32;
      let mut best_improvement = 0.0;
      for i in 0..(prefix_sequence.len() - 1) {
        let pref0 = &prefix_sequence[i];
        let pref1 = &prefix_sequence[i + 1];

        let improvement = combine_improvement(pref0, pref1, n);
        if improvement > best_improvement {
          can_improve = true;
          best_i = i as i32;
          best_improvement = improvement;
        }
      }

      if can_improve {
        let pref0 = &prefix_sequence[best_i as usize];
        let pref1 = &prefix_sequence[best_i as usize + 1];
        prefix_sequence[best_i as usize] = PrefixIntermediate::new(
          pref0.weight + pref1.weight,
          pref0.lower,
          pref1.upper,
        );
        //not the most efficient but whatever
        prefix_sequence.remove(best_i as usize + 1);
      }
    }

    huffman::make_huffman_code(&mut prefix_sequence);

    let mut prefixes = Vec::new();
    for p in &prefix_sequence {
      let upper = p.upper + 1;
      prefixes.push(Prefix::new(p.val.clone(), p.lower, upper));
    }

    return Ok(Compressor::new(prefixes, ints.len()));
  }

  pub fn compress_int(&self, i: i64) -> Vec<bool> {
    for pref in &self.prefixes {
      if pref.lower <= i && pref.upper > i {
        let mut res = pref.val.clone();
        let off = u64_diff(i, pref.lower);
        res.extend(u64_to_least_significant_bits(off, pref.k));
        if off < pref.km1min || off >= pref.km1max {
          res.push(((off >> pref.k) & 1) > 0) // most significant bit, if necessary, comes last
        }
        return res;
      }
    }
    panic!(format!("none of the ranges include i={}", i));
  }

  pub fn compress_ints(&self, ints: &Vec<i64>) -> Vec<bool> {
    return ints
      .iter()
      .flat_map(|i| self.compress_int(*i))
      .collect();
  }

  pub fn compression_data(&self) -> Vec<bool> {
    let mut res = Vec::new();
    res.extend(u32_to_bits(self.n, BITS_TO_ENCODE_N_ENTRIES));
    res.extend(u32_to_bits(self.prefixes.len(), MAX_MAX_DEPTH));
    for pref in &self.prefixes {
      res.extend(bytes_to_bits(pref.lower.to_be_bytes()));
      res.extend(bytes_to_bits(pref.upper.to_be_bytes()));
      res.extend(u32_to_bits(pref.val.len(), BITS_TO_ENCODE_PREFIX_LEN));
      res.extend(&pref.val);
    }
    return res;
  }

  pub fn compress_series(&self, ints: &Vec<i64>) -> Vec<bool> {
    let mut compression = self.compression_data();
    compression.append(&mut self.compress_ints(ints));
    return compression;
  }
}

impl Decompressor {
  pub fn new(prefixes: Vec<Prefix>, n: usize) -> Decompressor {
    let mut max_depth = 0;
    for p in &prefixes {
      max_depth = max(max_depth, p.val.len() as u32);
    }
    let n_pref = (1 as usize) << max_depth;
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

    Decompressor {
      prefixes,
      prefix_map,
      prefix_len_map,
      max_depth,
      n,
    }
  }

  pub fn from_bytes(bit_reader: &mut BitReader) -> Decompressor {
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

    let decompressor = Decompressor::new(prefixes, n);

    return decompressor;
  }

  pub fn decompress(&self, reader: &mut BitReader) -> Vec<i64> {
    let pow = (1 as usize) << self.max_depth;
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
      let range = u64_diff(upper, lower);
      let mut offset = reader.read_u64(k as usize);
      let most_significant = (1 as u64) << k;
      if range - offset > most_significant {
        if reader.read_one() {
          offset += most_significant;
        }
      }
      res.push(i64_plus_u64(lower, offset));
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

impl Display for Compressor {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    display_prefixes(&self.prefixes, f)
  }
}

impl Display for Decompressor {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    display_prefixes(&self.prefixes, f)
  }
}

