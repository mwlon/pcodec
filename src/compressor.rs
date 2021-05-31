use std::cmp::max;
use std::fmt;
use std::fmt::Display;

use crate::bit_reader::BitReader;
use crate::bits::*;
use crate::huffman;
use crate::prefix::{Prefix, PrefixIntermediate};

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


pub struct QuantileCompressor {
  prefixes: Vec<Prefix>,
  prefix_map: Vec<Option<Prefix>>, // used for decompression
  max_depth: u32, // used for decompression
}

impl QuantileCompressor {
  pub fn new(prefixes: Vec<Prefix>) -> QuantileCompressor {
    let mut max_depth = 0;
    for p in &prefixes {
      max_depth = max(max_depth, p.val.len() as u32);
    }
    let n_pref = (2 as usize).pow(max_depth);
    let mut prefix_map = Vec::new();
    for _ in 0..n_pref {
      prefix_map.push(None);
    }
    for p in &prefixes {
      let i = bits_to_usize_truncated(&p.val, max_depth);
      prefix_map[i] = Some(p.clone());
    }

    QuantileCompressor {
      prefixes,
      prefix_map,
      max_depth,
    }
  }

  pub fn from_bytes(bit_reader: &mut BitReader) -> QuantileCompressor {
    let n_pref = bits_to_usize(bit_reader.read(8));
    let mut prefixes = Vec::with_capacity(n_pref);
    for _ in 0..n_pref {
      let lower_bits = bit_reader.read(64);
      let lower = bits_to_int64(lower_bits);
      let upper_bits = bit_reader.read(64);
      let upper = bits_to_int64(upper_bits);
      let code_len_bits = bit_reader.read(4);
      let code_len = bits_to_usize(code_len_bits);
      let val = bit_reader.read(code_len);
      prefixes.push(Prefix::new(val, lower, upper));
    }

    let compressor = QuantileCompressor::new(prefixes);

    return compressor;
  }

  pub fn train(ints: &Vec<i64>, max_depth: u32) -> QuantileCompressor {
    let mut sorted = ints.clone();
    sorted.sort();
    let n = ints.len();
    let n_bucket = (2 as usize).pow(max_depth);
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

    return QuantileCompressor::new(prefixes);
  }

  pub fn compress_int(&self, i: i64) -> Vec<bool> {
    for pref in &self.prefixes {
      if pref.lower <= i && pref.upper > i {
        let mut res = pref.val.clone();
        let off = (i - pref.lower) as u64;
        let mut bits = u64_bytes_to_bits(off.to_be_bytes());
        let range_bitlen;
        if off >= pref.km1min && off < pref.km1max {
          range_bitlen = pref.k as usize - 1;
        } else {
          range_bitlen = pref.k as usize;
        }
        bits[64 - range_bitlen..64].reverse();
        res.extend(&bits[64 - range_bitlen..64]);
        return res;
      }
    }
    panic!(format!("none of the ranges include i={}", i));
  }

  pub fn compress_ints(&self, ints: &Vec<i64>) -> Vec<bool> {
    return ints
      .iter()
      .flat_map(|i| {
        let res = self.compress_int(*i);
        res
      })
      .collect();
  }

  pub fn compression_data(&self) -> Vec<bool> {
    let mut res = Vec::new();
    let mut n_pref = self.prefixes.len();
    let mut m = 128;
    for _ in 0..8 {
      res.push(n_pref >= m);
      n_pref %= m;
      m /= 2;
    }
    for pref in &self.prefixes {
      res.extend(u64_bytes_to_bits(pref.lower.to_be_bytes()));
      res.extend(u64_bytes_to_bits(pref.upper.to_be_bytes()));
      let mut x = pref.val.len();
      //max prefix len is 16
      let mut m = 8;
      for _ in 0..4 {
        res.push(x >= m);
        x %= m;
        m /= 2;
      }
      res.extend(&pref.val);
    }
    return res;
  }

  pub fn compress_series(&self, ints: &Vec<i64>) -> Vec<bool> {
    let mut compression = self.compression_data();
    compression.append(&mut self.compress_ints(ints));
    return compression;
  }

  pub fn decompress(&self, reader: &mut BitReader) -> Vec<i64> {
    let bits = reader.read_rest();

    let pow = (2 as usize).pow(self.max_depth);
    let mut i = 0;
    let mut res = Vec::new();
    let mut last_i = 0;
    loop {
      let mut maybe_p: Option<Prefix> = None;
      let mut p_idx = 0;
      let mut m = pow;
      for _ in 0..self.max_depth {
        if i >= bits.len() {
          return res;
        }

        m /= 2;
        p_idx += m * (bits[i] as usize);
        i += 1;
        let candidate = self.prefix_map[p_idx].clone();
        match candidate.clone() {
          Some(p) if p.val.len() == i - last_i => {
            maybe_p = candidate;
            break;
          },
          _ => ()
        }
      }
      let p = maybe_p.expect("couldn't find prefix");

      let mut mult = 1 as i64;
      let mut x = p.lower;
      for _ in 0..p.k - 1 {
        if i >= bits.len() {
          return res;
        }

        x += mult * (bits[i] as i64);
        i += 1;
        mult *= 2;
      }
      if u64_diff(p.upper, x) > mult as u64 {
        x += mult * (bits[i] as i64);
        i += 1;
      }

      last_i = i;
      res.push(x);
    }
  }
}

impl Display for QuantileCompressor {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let s = self.prefixes
      .iter()
      .map(|p| format!(
        "\t{}: {} to {} (density {})",
        bits_to_string(&p.val),
        p.lower,
        p.upper,
        2.0_f64.powf(-(p.val.len() as f64)) / (p.upper - p.lower) as f64
      ))
      .collect::<Vec<String>>()
      .join("\n");
    write!(f, "{}", s)
  }
}

