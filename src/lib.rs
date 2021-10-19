pub use bit_reader::BitReader;
pub use compressor::Compressor;
pub use decompressor::Decompressor;
pub use types::boolean::BoolCompressor;
pub use types::boolean::BoolDecompressor;
pub use types::float32::F32Compressor;
pub use types::float32::F32Decompressor;
pub use types::float64::F64Compressor;
pub use types::float64::F64Decompressor;
pub use types::signed32::I32Compressor;
pub use types::signed32::I32Decompressor;
pub use types::signed64::I64Compressor;
pub use types::signed64::I64Decompressor;
pub use types::timestamps::TimestampNs;
pub use types::timestamps::TimestampNsCompressor;
pub use types::timestamps::TimestampNsDecompressor;
pub use types::timestamps::TimestampMicros;
pub use types::timestamps::TimestampMicrosCompressor;
pub use types::timestamps::TimestampMicrosDecompressor;
pub use types::unsigned32::U32Compressor;
pub use types::unsigned32::U32Decompressor;
pub use types::unsigned64::U64Compressor;
pub use types::unsigned64::U64Decompressor;

pub use constants::MAX_ENTRIES;

mod bits;
mod constants;
mod huffman;
mod prefix;
mod utils;
pub mod bit_reader;
pub mod compressor;
pub mod decompressor;
pub mod errors;
pub mod types;

#[cfg(test)]
mod tests {
  use crate::types::NumberLike;
  use crate::{Compressor, BitReader, Decompressor, TimestampNs, TimestampMicros};

  #[test]
  fn test_edge_cases() {
    assert_recovers(vec![true, true, false, true], 0);
    assert_recovers(vec![false, false, false], 0);
    assert_recovers(vec![false], 0);
    assert_recovers(vec![u64::MIN, u64::MAX], 0);
    assert_recovers(vec![f64::MIN, f64::MAX], 0);
    assert_recovers(vec![1.2_f32], 0);
    assert_recovers(vec![1.2_f32], 1);
    assert_recovers(vec![1.2_f32], 2);
    assert_recovers(Vec::<u32>::new(), 6);
    assert_recovers(Vec::<u32>::new(), 0);
  }

  #[test]
  fn test_moderate_data() {
    let mut v = Vec::new();
    for i in -50000..50000 {
      v.push(i);
    }
    assert_recovers(v, 5);
  }

  #[test]
  fn test_boolean_codec() {
    assert_recovers(vec![true, true, false, true, false], 1);
  }

  #[test]
  fn test_sparse() {
    let mut v = Vec::new();
    for _ in 0..10000 {
      v.push(true);
    }
    v.push(false);
    v.push(false);
    v.push(true);
    assert_recovers(v, 1);
  }

  #[test]
  fn test_u32_codec() {
    assert_recovers(vec![0_u32, u32::MAX, 3, 4, 5], 1);
  }

  #[test]
  fn test_u64_codec() {
    assert_recovers(vec![0_u64, u64::MAX, 3, 4, 5], 1);
  }

  #[test]
  fn test_i32_codec() {
    assert_recovers(vec![0_i32, -1, i32::MAX, i32::MIN, 7], 1);
  }

  #[test]
  fn test_i64_codec() {
    assert_recovers(vec![0_i64, -1, i64::MAX, i64::MIN, 7], 1);
  }

  #[test]
  fn test_f32_codec() {
    assert_recovers(vec![f32::MAX, f32::MIN, f32::NAN, f32::NEG_INFINITY, f32::INFINITY, 0.0, 77.7], 1);
  }

  #[test]
  fn test_f64_codec() {
    assert_recovers(vec![f64::MAX, f64::MIN, f64::NAN, f64::NEG_INFINITY, f64::INFINITY, 0.0, 77.7], 1);
  }

  #[test]
  fn test_timestamp_ns_codec() {
    assert_recovers(
      vec![
        TimestampNs::from_secs_and_nanos(i64::MIN, 0),
        TimestampNs::from_secs_and_nanos(i64::MAX, 999_999_999),
        TimestampNs::from_secs_and_nanos(i64::MIN, 999_999_999),
        TimestampNs::from_secs_and_nanos(0, 123_456_789),
        TimestampNs::from_secs_and_nanos(-1, 123_456_789),
      ],
      1
    );
  }

  #[test]
  fn test_timestamp_micros_codec() {
    assert_recovers(
      vec![
        TimestampMicros::from_secs_and_nanos(i64::MIN, 0),
        TimestampMicros::from_secs_and_nanos(i64::MAX, 999_999_999),
        TimestampMicros::from_secs_and_nanos(i64::MIN, 999_999_999),
        TimestampMicros::from_secs_and_nanos(0, 123_456_789),
        TimestampMicros::from_secs_and_nanos(-1, 123_456_789),
      ],
      1
    );
  }

  fn assert_recovers<T: NumberLike>(vals: Vec<T>, max_depth: u32) {
    let compressor = Compressor::train(vals.clone(), max_depth)
      .expect("training error");
    let compressed = compressor.compress(&vals)
      .expect("compression error");
    let mut bit_reader = BitReader::from(compressed);
    let decompressor = Decompressor::<T>::from_reader(&mut bit_reader)
      .expect("header error");
    let decompressed = decompressor.decompress(&mut bit_reader);
    // can't do assert_eq on the whole vector because floating points don't compare exactly
    assert_eq!(decompressed.len(), vals.len());
    for i in 0..decompressed.len() {
      assert!(decompressed[i].num_eq(&vals[i]));
    }
  }
}
