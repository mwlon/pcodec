use crate::chunk_config::ChunkConfig;

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
  let fc = FileCompressor::default();
  let chunk_config = ChunkConfig {
    compression_level,
    ..Default::default()
  };
  let cc = fc.chunk_compressor(nums, &chunk_config)?;
  Ok(cc.meta().delta_encoding_order)
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
