use std::cmp::max;
use std::collections::HashMap;
use std::fmt::Debug;

use rand_xoshiro::rand_core::{RngCore, SeedableRng};

use crate::bits::ceil_div;
use crate::data_types::Latent;

pub const MIN_SAMPLE: usize = 10;
// 1 in this many nums get put into sample
const SAMPLE_RATIO: usize = 40;
// Int mults will be considered infrequent if they occur less than 1/this of
// the time.
const CLASSIC_MEMORIZATION_THRESH: f64 = 256.0;
// what proportion of numbers must come from infrequent mults
const INFREQUENT_MULT_WEIGHT_THRESH: f64 = 0.04;
// how many times over to try collecting samples without replacement before
// giving up
const SAMPLING_PERSISTENCE: usize = 4;

fn calc_sample_n(n: usize) -> Option<usize> {
  if n >= MIN_SAMPLE {
    Some(MIN_SAMPLE + (n - MIN_SAMPLE) / SAMPLE_RATIO)
  } else {
    None
  }
}

#[inline(never)]
pub fn choose_sample<T, S: Copy + Debug, Filter: Fn(&T) -> Option<S>>(
  nums: &[T],
  filter: Filter,
) -> Option<Vec<S>> {
  // We can't modify the list, and copying it may be expensive, but we want to
  // sample a small fraction from it without replacement, so we keep a
  // bitpacked vector representing whether each one is used yet and just keep
  // resampling.
  // Maybe this is a bad idea, but it works for now.
  let target_sample_size = calc_sample_n(nums.len())?;

  let mut rng = rand_xoshiro::Xoroshiro128PlusPlus::seed_from_u64(0);
  let mut visited = vec![0_u8; ceil_div(nums.len(), 8)];
  let mut res = Vec::with_capacity(target_sample_size);
  let mut n_iters = 0;
  while res.len() < target_sample_size && n_iters < SAMPLING_PERSISTENCE * target_sample_size {
    let rand_idx = rng.next_u64() as usize % nums.len();
    let visited_idx = rand_idx / 8;
    let visited_bit = rand_idx % 8;
    let mask = 1 << visited_bit;
    let is_visited = visited[visited_idx] & mask;
    if is_visited == 0 {
      if let Some(x) = filter(&nums[rand_idx]) {
        res.push(x);
      }
      visited[visited_idx] |= mask;
    }
    n_iters += 1;
  }

  if res.len() >= MIN_SAMPLE {
    Some(res)
  } else {
    None
  }
}

#[inline(never)]
pub fn has_enough_infrequent_ints<L: Latent, S: Copy, F: Fn(S) -> L>(
  sample: &[S],
  mult_fn: F,
) -> bool {
  let mut mult_counts = HashMap::<L, usize>::with_capacity(sample.len());
  for &x in sample {
    let mult = mult_fn(x);
    *mult_counts.entry(mult).or_default() += 1;
  }

  let infrequent_cutoff = max(
    1,
    (sample.len() as f64 / CLASSIC_MEMORIZATION_THRESH) as usize,
  );

  // Maybe this should be made fuzzy instead of a hard cutoff because it's just
  // a sample.
  let infrequent_mult_weight_estimate = mult_counts
    .values()
    .filter(|&&count| count <= infrequent_cutoff)
    .sum::<usize>();
  (infrequent_mult_weight_estimate as f64 / sample.len() as f64) > INFREQUENT_MULT_WEIGHT_THRESH
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_sample_n() {
    assert_eq!(calc_sample_n(9), None);
    assert_eq!(calc_sample_n(10), Some(10));
    assert_eq!(calc_sample_n(100), Some(12));
    assert_eq!(calc_sample_n(1000010), Some(25010));
  }

  #[test]
  fn test_choose_sample() {
    let mut nums = Vec::new();
    for i in 0..150 {
      nums.push(-i as f32);
    }
    let mut sample = choose_sample(&nums, |&num| {
      if num == 0.0 {
        None
      } else {
        Some(num)
      }
    })
    .unwrap();
    sample.sort_unstable_by(f32::total_cmp);
    assert_eq!(sample.len(), 13);
    assert_eq!(&sample[0..3], &[-147.0, -142.0, -119.0]);
  }
}
