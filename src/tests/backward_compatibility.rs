use std::fs;

use crate::Decompressor;
use crate::data_types::{NumberLike, TimestampMicros};

fn assert_compatible<T: NumberLike>(
  filename: &str,
) {
  let raw_bytes = fs::read(format!("assets/{}.bin", filename)).expect("read bin");
  let expected = raw_bytes
    .chunks(T::PHYSICAL_BITS / 8)
    .map(|chunk| T::from_bytes(chunk.to_vec()).expect("raw corruption"))
    .collect::<Vec<_>>();

  let compressed = fs::read(format!("assets/{}.qco", filename)).expect("read qco");
  let decompressor = Decompressor::<T>::default();
  let decompressed = decompressor.simple_decompress(compressed).expect("decompress");

  assert_eq!(decompressed, expected)
}

#[test]
fn test_v04_empty() {
  assert_compatible::<i64>("v0.4_i64_empty");
}

#[test]
fn test_v04_bool_sparse() {
  assert_compatible::<bool>("v0.4_bool_sparse_2k");
}

#[test]
fn test_v04_i32() {
  assert_compatible::<i32>("v0.4_i32_2k");
}

#[test]
fn test_v04_f32() {
  assert_compatible::<f32>("v0.4_f32_2k");
}

#[test]
fn test_v06_timestamp_deltas() {
  assert_compatible::<TimestampMicros>("v0.6_timestamp_deltas_2k");
}