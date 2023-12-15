use std::cmp::{max, min};
use std::collections::HashMap;

use crate::data_types::UnsignedLike;

const MIN_SAMPLE: usize = 10;
// 1 in this many nums get put into sample
const SAMPLE_RATIO: usize = 40;
const SAMPLE_SIN_PERIOD: usize = 16;
// Int mults will be considered infrequent if they occur less than 1/this of
// the time.
const CLASSIC_MEMORIZATION_THRESH: f64 = 256.0;
// what proportion of numbers must come from infrequent mults
const INFREQUENT_MULT_WEIGHT_THRESH: f64 = 0.05;

fn calc_sample_n(n: usize) -> Option<usize> {
  if n >= MIN_SAMPLE {
    Some(MIN_SAMPLE + (n - MIN_SAMPLE) / SAMPLE_RATIO)
  } else {
    None
  }
}

pub fn choose_sample<T, S, Filter: Fn(&T) -> Option<S>>(
  nums: &[T],
  filter: Filter,
) -> Option<Vec<S>> {
  // pick evenly-spaced nums
  let n = nums.len();
  let sample_n = calc_sample_n(n)?;

  // we avoid cyclic sampling by throwing in another frequency
  let slope = n as f64 / sample_n as f64;
  let sin_rate = std::f64::consts::TAU / (SAMPLE_SIN_PERIOD as f64);
  let sins: [f64; SAMPLE_SIN_PERIOD] = core::array::from_fn(|i| (i as f64 * sin_rate).sin() * 0.5);
  let res = (0..sample_n)
    .flat_map(|i| {
      let idx = ((i as f64 + sins[i % 16]) * slope) as usize;

      filter(&nums[min(idx, n - 1)])
    })
    .collect::<Vec<_>>();

  if res.len() > MIN_SAMPLE {
    Some(res)
  } else {
    None
  }
}

pub fn has_enough_infrequent_ints<U: UnsignedLike, S: Copy, F: Fn(S) -> U>(
  sample: &[S],
  mult_fn: F,
) -> bool {
  let mut mult_counts = HashMap::<U, usize>::with_capacity(sample.len());
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
    let sample = choose_sample(&nums, |&num| {
      if num == 0.0 {
        None
      } else {
        Some(num)
      }
    })
    .unwrap();
    assert_eq!(sample.len(), 12);
    assert_eq!(&sample[0..3], &[-13.0, -27.0, -39.0]);
  }
}
