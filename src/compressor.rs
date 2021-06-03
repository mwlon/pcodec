use std::cmp::max;
use std::fmt;
use std::fmt::Display;

use crate::bits::*;
use crate::huffman;
use crate::int64::*;
use crate::prefix::{Prefix, PrefixIntermediate};
use crate::utils;

fn combine_improvement(p0: &PrefixIntermediate, p1: &PrefixIntermediate, n: usize) -> f64 {
  let p0_r_cost = avg_base2_bits(p0.upper, p0.lower);
  let p1_r_cost = avg_base2_bits(p1.upper, p1.lower);
  let combined_r_cost = avg_base2_bits(p1.upper, p0.lower);
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

pub struct I64Compressor {
  prefixes: Vec<Prefix>,
  n: usize,
}

impl I64Compressor {
  pub fn new(prefixes: Vec<Prefix>, n: usize) -> I64Compressor {
    I64Compressor {
      prefixes,
      n,
    }
  }

  pub fn train(ints: &Vec<i64>, max_depth: u32) -> Result<I64Compressor, String> {
    if max_depth > MAX_MAX_DEPTH {
      return Err(format!("max depth cannot exceed {}", MAX_MAX_DEPTH));
    }
    if ints.len() as u64 > MAX_ENTRIES {
      return Err(format!("number of entries cannot exceed {}", MAX_ENTRIES));
    }

    let mut sorted = ints.clone();
    sorted.sort();
    let n = ints.len();
    let n_bucket = (1_usize) << max_depth;
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
      prefixes.push(Prefix::new(p.val.clone(), p.lower, p.upper));
    }

    return Ok(I64Compressor::new(prefixes, ints.len()));
  }

  pub fn compress_int(&self, i: i64) -> Vec<bool> {
    for pref in &self.prefixes {
      if pref.lower <= i && pref.upper >= i {
        let mut res = pref.val.clone();
        let off = utils::u64_diff(i, pref.lower);
        res.extend(u64_to_least_significant_bits(off, pref.k));
        if off < pref.only_k_bits_lower || off > pref.only_k_bits_upper {
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

  pub fn compress_sequence(&self, ints: &Vec<i64>) -> Vec<bool> {
    let mut compression = self.compression_data();
    compression.append(&mut self.compress_ints(ints));
    return compression;
  }
}

impl Display for I64Compressor {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    utils::display_prefixes(&self.prefixes, f)
  }
}
