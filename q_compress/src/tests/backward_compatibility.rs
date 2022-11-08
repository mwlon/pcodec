use std::fs;

use crate::{auto_decompress, DecompressorConfig};
use crate::data_types::NumberLike;
#[cfg(feature = "timestamps_96")]
use crate::data_types::TimestampMicros96;
use crate::tests::utils;

#[derive(Clone, Copy, Debug)]
enum Mode {
  Standalone,
  Wrapped,
}

fn assert_compatible<T: NumberLike>(
  filename: &str,
  mode: Mode,
) {
  let raw_bytes = fs::read(format!("assets/{}.bin", filename)).expect("read bin");
  let expected = raw_bytes
    .chunks(T::PHYSICAL_BITS / 8)
    .map(|chunk| T::from_bytes(chunk.to_vec()).expect("raw corruption"))
    .collect::<Vec<_>>();

  let compressed = fs::read(format!("assets/{}.qco", filename)).expect("read qco");
  let decompressed = match mode {
    Mode::Standalone => auto_decompress::<T>(&compressed),
    Mode::Wrapped => utils::wrapped_decompress(compressed, DecompressorConfig::default()),
  }.expect("decompress");

  assert_eq!(decompressed, expected)
}

#[test]
fn test_v0_4_empty() {
  assert_compatible::<i64>("v0.4_i64_empty", Mode::Standalone);
}

#[test]
fn test_v0_4_bool_sparse() {
  assert_compatible::<bool>("v0.4_bool_sparse_2k", Mode::Standalone);
}

#[test]
fn test_v0_4_i32() {
  assert_compatible::<i32>("v0.4_i32_2k", Mode::Standalone);
}

#[test]
fn test_v0_4_f32() {
  assert_compatible::<f32>("v0.4_f32_2k", Mode::Standalone);
}

#[cfg(feature = "timestamps_96")]
#[test]
fn test_v0_6_timestamp_deltas() {
  assert_compatible::<TimestampMicros96>("v0.6_timestamp_deltas_2k", Mode::Standalone);
}

#[test]
fn test_v0_9_dispersed_shorts() {
  assert_compatible::<u16>("v0.9_dispersed_shorts", Mode::Standalone);
}

#[test]
fn test_v0_10_varied_gcds() {
  assert_compatible::<f32>("v0.10_varied_gcds", Mode::Standalone);
}

#[test]
fn test_v0_10_same_gcds() {
  assert_compatible::<i32>("v0.10_same_gcds", Mode::Standalone);
}

#[test]
fn test_v0_11_2_wrapped_brownian() {
  assert_compatible::<f32>("v0.11.2_wrapped_brownian", Mode::Wrapped);
}
