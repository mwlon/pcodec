use std::cmp::min;
use std::io::Write;

use crate::{Compressor, CompressorConfig, Decompressor};
use crate::data_types::NumberLike;
use crate::errors::QCompressResult;

const AUTO_DELTA_LIMIT: usize = 1000;

/// Automatically makes an educated guess for the best compression
/// configuration, based on `nums` and `compression_level`,
/// then compresses the numbers to bytes.
///
/// This adds some compute cost by trying different configurations on a subset
/// of the numbers to determine the most likely one to do well.
/// If you know what configuration you want ahead of time (namely delta
/// encoding order), you can use [`Compressor::from_config`] instead to spare
/// the compute cost.
/// See [`CompressorConfig`] for information about compression levels.
pub fn auto_compress<T: NumberLike>(nums: &[T], compression_level: usize) -> Vec<u8> {
  let mut compressor = Compressor::from_config(auto_compressor_config(nums, compression_level));
  compressor.simple_compress(nums)
}

/// Automatically makes an educated guess for the best decompression
/// configuration, then decompresses the bytes into numbers.
///
/// There are currently no relevant fields in the decompression configuration,
/// so there is no compute downside to using this function.
pub fn auto_decompress<T: NumberLike>(bytes: &[u8]) -> QCompressResult<Vec<T>> {
  let mut decompressor = Decompressor::<T>::default();
  decompressor.write_all(bytes).unwrap();
  decompressor.simple_decompress()
}

/// Automatically makes an educated guess for the best compression
/// configuration, based on `nums` and `compression_level`.
///
/// This has some compute cost by trying different configurations on a subset
/// of the numbers to determine the most likely one to do well.
/// See [`CompressorConfig`] for information about compression levels.
pub fn auto_compressor_config<T: NumberLike>(nums: &[T], compression_level: usize) -> CompressorConfig {
  let delta_encoding_order = auto_delta_encoding_order(nums, compression_level);
  CompressorConfig::default()
    .with_compression_level(compression_level)
    .with_delta_encoding_order(delta_encoding_order)
}

fn auto_delta_encoding_order<T: NumberLike>(
  nums: &[T],
  compression_level: usize,
) -> usize {
  let head_nums = if nums.len() < AUTO_DELTA_LIMIT {
    nums
  } else {
    &nums[0..AUTO_DELTA_LIMIT]
  };
  let mut best_order = usize::MAX;
  let mut best_size = usize::MAX;
  for delta_encoding_order in 0..8 {
    // Taking deltas of a large dataset won't change the GCD,
    // so we don't need to waste compute here inferring GCD's just to
    // determine the best delta order.
    let config = CompressorConfig::default()
      .with_delta_encoding_order(delta_encoding_order)
      .with_compression_level(min(compression_level, 6))
      .with_use_gcds(false);
    let mut compressor = Compressor::<T>::from_config(config);
    compressor.header().unwrap();
    compressor.chunk(head_nums).unwrap(); // only unreachable errors
    let size = compressor.byte_size();
    if size < best_size {
      best_order = delta_encoding_order;
      best_size = size;
    } else {
      // it's almost always monotonic
      break;
    }
  }
  best_order
}

#[cfg(test)]
mod tests {
  use crate::auto::auto_delta_encoding_order;

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
    assert_eq!(auto_delta_encoding_order(&linear_trend, 3), 1);
    assert_eq!(auto_delta_encoding_order(&quadratic_trend, 3), 2);
  }
}
