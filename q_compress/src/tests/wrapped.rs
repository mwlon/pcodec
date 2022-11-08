use crate::{CompressorConfig, DecompressorConfig};
use crate::errors::QCompressResult;
use crate::tests::utils;

#[test]
fn test_dummy_wrapped_format_recovery() -> QCompressResult<()> {
  let nums = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
  let config = CompressorConfig {
    delta_encoding_order: 2,
    ..Default::default()
  };
  let sizess = vec![vec![4, 2, 1], vec![3]];
  let compressed = utils::wrapped_compress(&nums, config, sizess)?;
  let recovered = utils::wrapped_decompress::<i32>(compressed, DecompressorConfig::default())?;
  assert_eq!(recovered, nums);
  Ok(())
}