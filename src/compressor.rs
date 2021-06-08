use std::cmp::{max, min};
use std::fmt;
use std::fmt::Display;
use std::marker::PhantomData;

use crate::bits::*;
use crate::huffman;
use crate::prefix::{Prefix, PrefixIntermediate};
use crate::types::{DataType, NumberLike};
use crate::utils;
use crate::utils::*;
use std::cmp::Ordering::{Greater, Less};

const MIN_N_TO_USE_REPS: usize = 1000;
const MIN_FREQUENCY_TO_USE_REPS: f64 = 0.8;
const MIN_REPS_RATIO: f64 = 3.0;

struct RepConfiguration {
  weight: u64,
  reps: usize,
}

fn choose_rep_configs(
  weight: u64,
  n: u64,
  max_buckets: usize,
) -> Vec<RepConfiguration> {
  // This strategy could probably be improved in the future.
  // Current idea is to simply choose the tokens that could describe a
  // run of the common number in as few tokens as possible.
  // But this ignores that some tokens will have longer Huffman codes,
  // so we could do better in the future by taking that into account.
  let freq = (weight as f64) / (n as f64);
  let mean_run_len = 1.0 / (1.0 - freq);
  let mut res = Vec::new();

  // how many buckets we'll actually use
  // it's not usually efficient to make a new prefix when <4 copies
  let n_buckets = min(
    max_buckets,
    (mean_run_len.log2() / MIN_REPS_RATIO.log2()).floor() as usize
  );
  let ratio = mean_run_len.powf(1.0 / n_buckets as f64);
  let mut s = 0.0;
  for i in 0..n_buckets {
    s += ratio.powf(i as f64);
  }
  let new_weight = ((weight as f64) / s).floor() as u64;
  let rep_1_new_weight = new_weight + 1;
  res.push(RepConfiguration {
    reps: 1,
    weight: rep_1_new_weight,
  });
  for i in 1..n_buckets {
    res.push(RepConfiguration {
      reps: ratio.powf(i as f64).floor() as usize,
      weight: new_weight,
    });
  }
  res
}

fn push_pref<T: Copy>(
  seq: &mut Vec<PrefixIntermediate<T>>,
  bucket_idx: &mut usize,
  i: usize,
  j: usize,
  n_bucket: usize,
  n: usize,
  sorted: &Vec<T>,
) {
  let weight = j - i;
  let frequency = weight as f64 / n as f64;
  let new_bucket_idx = max(*bucket_idx + 1, (j * n_bucket) / n);
  let bucket_idx_incr = new_bucket_idx - *bucket_idx;
  if n < MIN_N_TO_USE_REPS || frequency < MIN_FREQUENCY_TO_USE_REPS || weight == n || bucket_idx_incr == 1 {
    // The usual case - a prefix for a range that represents either 100% or
    // <=80% of the data.
    // This also works if there are no other ranges.
    seq.push(PrefixIntermediate::new(weight as u64, sorted[i], sorted[j - 1], 1));
  } else {
    // The weird case - a range that represents almost all (but not all) the data.
    // We create extra prefixes that can describe `reps` copies of the range at once.
    let rep_configs = choose_rep_configs(weight as u64, n as u64, bucket_idx_incr);
    for config in &rep_configs {
      seq.push(PrefixIntermediate::new(config.weight, sorted[i], sorted[j - 1], config.reps));
    }
  }
  *bucket_idx = new_bucket_idx;
}

pub struct Compressor<T, DT> where T: NumberLike, DT: DataType<T> {
  prefixes: Vec<Prefix<T>>,
  n: usize,
  data_type: PhantomData<DT>,
}

impl<T, DT> Compressor<T, DT> where T: NumberLike, DT: DataType<T> {
  pub fn train(nums: &Vec<T>, max_depth: u32) -> Result<Self, String> {
    if max_depth > MAX_MAX_DEPTH {
      return Err(format!("max depth cannot exceed {}", MAX_MAX_DEPTH));
    }
    if nums.len() as u64 > MAX_ENTRIES {
      return Err(format!("number of entries cannot exceed {}", MAX_ENTRIES));
    }

    let mut sorted = nums.clone();
    sorted.sort();
    let n = nums.len();
    let n_bucket = 1_usize << max_depth;
    let mut prefix_sequence: Vec<PrefixIntermediate<T>> = Vec::new();
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

        let improvement = Self::combine_improvement(pref0, pref1, n);
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
          1,
        );
        //not the most efficient but whatever
        prefix_sequence.remove(best_i as usize + 1);
      }
    }

    huffman::make_huffman_code(&mut prefix_sequence);

    let mut prefixes = Vec::new();
    for p in &prefix_sequence {
      prefixes.push(Prefix::new(p.val.clone(), p.lower, p.upper, DT::u64_diff(p.upper, p.lower), p.reps));
    }

    let res = Compressor::<T, DT> {
      prefixes,
      n: nums.len(),
      data_type: PhantomData,
    };
    return Ok(res);
  }

  pub fn combine_improvement(p0: &PrefixIntermediate<T>, p1: &PrefixIntermediate<T>, n: usize) -> f64 {
    if p0.reps != 1 || p1.reps != 1 {
      // can never combine prefixes with multiple reps
      return f64::MIN;
    }

    let p0_r_cost = avg_base2_bits(DT::u64_diff(p0.upper, p0.lower));
    let p1_r_cost = avg_base2_bits(DT::u64_diff(p1.upper, p1.lower));
    let combined_r_cost = avg_base2_bits(DT::u64_diff(p1.upper, p0.lower));
    let p0_d_cost = depth_bits(p0.weight, n);
    let p1_d_cost = depth_bits(p1.weight, n);
    let combined_d_cost = depth_bits(p0.weight + p1.weight, n);
    let meta_cost = 10.0 + 2.0 * DT::BIT_SIZE as f64;

    let separate_cost = 2.0 * meta_cost +
      (p0_r_cost + p0_d_cost) * p0.weight as f64+
      (p1_r_cost + p1_d_cost) * p1.weight as f64;
    let combined_cost = meta_cost +
      (combined_r_cost + combined_d_cost) * (p0.weight + p1.weight) as f64;
    let bits_saved = separate_cost - combined_cost;
    return bits_saved as f64;
  }

  #[inline(always)]
  fn compress_num_offset_bits_w_prefix(&self, num: T, pref: &Prefix<T>, v: &mut Vec<bool>) {
    let off = DT::u64_diff(num, pref.lower);
    v.extend(u64_to_bits(off, pref.k));
    if off < pref.only_k_bits_lower || off > pref.only_k_bits_upper {
      v.push(((off >> pref.k) & 1) > 0) // most significant bit, if necessary, comes last
    }
  }

  #[inline(always)]
  fn compress_num_as_bits_w_prefix(&self, num: T, pref: &Prefix<T>, v: &mut Vec<bool>) {
    v.extend(&pref.val);
    self.compress_num_offset_bits_w_prefix(num, pref, v);
  }

  pub fn compress_num_as_bits(&self, num: T) -> Vec<bool> {
    for pref in &self.prefixes {
      if pref.upper >= num && pref.lower <= num && pref.reps == 1 {
        let mut res = Vec::new();
        self.compress_num_as_bits_w_prefix(num, &pref, &mut res);
        return res;
      }
    }
    panic!(format!("none of the ranges include i={}", num));
  }

  pub fn compress_nums_as_bits(&self, nums: &Vec<T>) -> Vec<bool> {
    let mut sorted_prefixes = self.prefixes.clone();
    // most reps comes first
    sorted_prefixes.sort_by(
      |p0, p1|
        if p0.reps > p1.reps {
          Less
        } else if p0.reps > p1.reps {
          Greater
        } else { // same # of reps, order by frequency
          if p0.val.len() < p1.val.len() {
            Less
          } else {
            Greater
          }
        });

    let mut res = Vec::new();
    let res_ptr = &mut res;
    let mut i = 0;
    while i < nums.len() {
      let mut success = false;
      for pref in &sorted_prefixes {
        let reps = pref.reps;
        if i + pref.reps > nums.len() {
          continue;
        }

        let mut matches = true;
        for i_prime in i..i + reps {
          let x = nums[i_prime];
          if x < pref.lower || x > pref.upper {
            matches = false;
            break;
          }
        }

        if matches {
          res_ptr.extend(&pref.val);
          for i_prime in i..i + reps {
            self.compress_num_offset_bits_w_prefix(nums[i_prime], &pref, res_ptr);
          }
          i += reps;
          success = true;
          break;
        }
      }

      if !success {
        panic!(format!("failed to compress some numbers! {}th: {}", i, nums[i]));
      }
    }
    res
  }

  pub fn metadata_as_bits(&self) -> Vec<bool> {
    let mut res = Vec::new();
    res.extend(bytes_to_bits(MAGIC_HEADER.to_vec()));
    res.extend(&byte_to_bits(DT::HEADER_BYTE));
    res.extend(usize_to_bits(self.n, BITS_TO_ENCODE_N_ENTRIES));
    res.extend(usize_to_bits(self.prefixes.len(), MAX_MAX_DEPTH));
    for pref in &self.prefixes {
      res.extend(bytes_to_bits(DT::bytes_from(pref.lower)));
      res.extend(bytes_to_bits(DT::bytes_from(pref.upper)));
      res.extend(usize_to_bits(pref.val.len(), BITS_TO_ENCODE_PREFIX_LEN));
      res.extend(&pref.val);
      if pref.reps == 1 {
        res.push(false);
      } else {
        res.push(true);
        res.extend(usize_to_bits(pref.reps, BITS_TO_ENCODE_REPS));
      }
    }
    return res;
  }

  pub fn compress_as_bits(&self, ints: &Vec<T>) -> Vec<bool> {
    let mut compression = self.metadata_as_bits();
    compression.append(&mut self.compress_nums_as_bits(ints));
    return compression;
  }

  pub fn compress(&self, ints: &Vec<T>) -> Vec<u8> {
    return bits_to_bytes(self.compress_as_bits(ints));
  }
}

impl<T, DT> Display for Compressor<T, DT> where T: NumberLike, DT: DataType<T> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    utils::display_prefixes(&self.prefixes, f)
  }
}
