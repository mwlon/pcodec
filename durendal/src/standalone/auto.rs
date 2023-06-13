use std::cmp::min;

use crate::constants::{AUTO_DELTA_LIMIT, MAX_AUTO_DELTA_COMPRESSION_LEVEL};
use crate::data_types::NumberLike;
use crate::errors::QCompressResult;
use crate::standalone::simple::simple_decompress;
use crate::standalone::{simple_compress, Compressor};
use crate::{CompressorConfig, DecompressorConfig};

/// Automatically makes an educated guess for the best compression
/// configuration, based on `nums` and `compression_level`,
/// then compresses the numbers to .qco bytes.
///
/// This adds some compute cost by trying different configurations on a subset
/// of the numbers to determine the most likely one to do well.
/// If you know what configuration you want ahead of time (namely delta
/// encoding order), you can use [`Compressor::from_config`] instead to spare
/// the compute cost.
/// See [`CompressorConfig`] for information about compression levels.
pub fn auto_compress<T: NumberLike>(nums: &[T], compression_level: usize) -> Vec<u8> {
  let config = auto_compressor_config(nums, compression_level);
  simple_compress(config, nums)
}

/// Automatically makes an educated guess for the best decompression
/// configuration, then decompresses .qco bytes into numbers.
///
/// There are currently no relevant fields in the decompression configuration,
/// so there is no compute downside to using this function.
pub fn auto_decompress<T: NumberLike>(bytes: &[u8]) -> QCompressResult<Vec<T>> {
  simple_decompress(DecompressorConfig::default(), bytes)
}

/// Automatically makes an educated guess for the best compression
/// configuration, based on `nums` and `compression_level`.
///
/// This has some compute cost by trying different configurations on a subset
/// of the numbers to determine the most likely one to do well.
/// See [`CompressorConfig`] for information about compression levels.
pub fn auto_compressor_config<T: NumberLike>(
  nums: &[T],
  compression_level: usize,
) -> CompressorConfig {
  let delta_encoding_order = auto_delta_encoding_order(nums, compression_level);
  CompressorConfig {
    compression_level,
    delta_encoding_order,
    ..Default::default()
  }
}

fn auto_delta_encoding_order<T: NumberLike>(nums: &[T], compression_level: usize) -> usize {
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
    let config = CompressorConfig {
      delta_encoding_order,
      compression_level: min(
        compression_level,
        MAX_AUTO_DELTA_COMPRESSION_LEVEL,
      ),
      use_gcds: false,
      use_float_mult: true,
    };
    let mut compressor = Compressor::<T>::from_config(config);
    compressor.header().unwrap();
    compressor.chunk(head_nums).unwrap(); // only unreachable errors
    let size = compressor.byte_size();

    if size < best_size {
      best_order = delta_encoding_order;
      best_size = size;
    } else {
      // it's almost always convex
      break;
    }
  }
  best_order
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
    assert_eq!(auto_delta_encoding_order(&no_trend, 3), 0);
    assert_eq!(
      auto_delta_encoding_order(&linear_trend, 3),
      1
    );
    assert_eq!(
      auto_delta_encoding_order(&quadratic_trend, 3),
      2
    );
  }

  #[test]
  fn test_auto_delta_encoding_order_step() {
    let mut nums = Vec::with_capacity(2000);
    for _ in 0..1000 {
      nums.push(77);
    }
    for _ in 1000..2000 {
      nums.push(78);
    }
    assert_eq!(auto_delta_encoding_order(&nums, 3), 1);
  }
}
