use std::cmp::min;

use crate::chunk_config::{ChunkConfig, PagingSpec};
use crate::constants::{AUTO_DELTA_LIMIT, MAX_AUTO_DELTA_COMPRESSION_LEVEL};
use crate::data_types::NumberLike;
use crate::errors::PcoResult;
use crate::wrapped::FileCompressor;

/// Automatically makes an educated guess for the best compression
/// delta encoding order, based on `nums` and `compression_level`.
///
/// This has some compute cost by trying different configurations on a subset
/// of the numbers to determine the most likely one to do well.
/// See [`ChunkConfig`] for information about compression levels.
pub fn auto_delta_encoding_order<T: NumberLike>(
  nums: &[T],
  compression_level: usize,
) -> PcoResult<usize> {
  let mut sampled_nums;
  let head_nums = if nums.len() < AUTO_DELTA_LIMIT {
    nums
  } else {
    // We take nums from start and maybe the end.
    // If the first numbers are all constant, we need to sample from the end.
    // Otherwise we'll do well enough by just using the start.
    let half_limit = AUTO_DELTA_LIMIT / 2;
    sampled_nums = Vec::with_capacity(AUTO_DELTA_LIMIT);
    sampled_nums.extend(&nums[..half_limit]);
    let zeroth_num = sampled_nums[0];
    if sampled_nums.iter().all(|num| *num == zeroth_num) {
      sampled_nums.extend(&nums[nums.len() - half_limit..]);
    } else {
      sampled_nums.extend(&nums[half_limit..AUTO_DELTA_LIMIT]);
    }
    &sampled_nums
  };

  let mut best_order = usize::MAX;
  let mut best_size = usize::MAX;
  for delta_encoding_order in 0..8 {
    // Taking deltas of a large dataset won't change the GCD,
    // so we don't need to waste compute here inferring GCD's just to
    // determine the best delta order.
    let config = ChunkConfig {
      delta_encoding_order: Some(delta_encoding_order),
      compression_level: min(
        compression_level,
        MAX_AUTO_DELTA_COMPRESSION_LEVEL,
      ),
      use_gcds: false,
      use_float_mult: true,
      paging_spec: PagingSpec::default(),
    };
    let fc = FileCompressor::default();
    let cc = fc.chunk_compressor(head_nums, &config)?;
    let size_estimate = cc.chunk_meta_size_hint() + cc.page_size_hint(0);
    let mut dst = vec![0; size_estimate];
    let mut consumed = cc.write_chunk_meta_sliced(&mut dst)?;
    consumed += cc.write_page_sliced(0, &mut dst[consumed..])?;

    if consumed < best_size {
      best_order = delta_encoding_order;
      best_size = consumed;
    } else {
      // it's almost always convex
      break;
    }
  }

  Ok(best_order)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_auto_delta_encoding_order() {
    let mut no_trend = Vec::new();
    let mut linear_trend = Vec::new();
    let mut quadratic_trend = Vec::new();
    let mut m = 1;
    for i in 0_i32..100_i32 {
      no_trend.push(m);
      m *= 77;
      m %= 100;
      linear_trend.push(i);
      quadratic_trend.push(i * i);
    }
    assert_eq!(
      auto_delta_encoding_order(&no_trend, 3).unwrap(),
      0
    );
    assert_eq!(
      auto_delta_encoding_order(&linear_trend, 3).unwrap(),
      1
    );
    assert_eq!(
      auto_delta_encoding_order(&quadratic_trend, 3).unwrap(),
      2
    );
  }

  #[test]
  fn test_auto_delta_encoding_order_step() {
    let mut nums = Vec::with_capacity(2000);
    nums.resize(1000, 77);
    nums.resize(2000, 78);
    assert_eq!(
      auto_delta_encoding_order(&nums, 3).unwrap(),
      1
    );
  }
}
