pub use bit_reader::BitReader;
pub use bit_writer::BitWriter;
pub use chunk_metadata::{ChunkMetadata, DecompressedChunk};
pub use compressor::{Compressor, CompressorConfig};
pub use decompressor::{Decompressor, DecompressorConfig};
pub use flags::Flags;
pub use types::timestamps::{TimestampMicros, TimestampNanos};

mod bits;
mod chunk_metadata;
mod constants;
mod delta_encoding;
mod flags;
mod huffman_decoding;
mod huffman_encoding;
mod prefix;
pub mod bit_reader;
pub mod bit_writer;
pub mod compressor;
pub mod decompressor;
pub mod errors;
pub mod types;

#[cfg(test)]
mod tests {
  use crate::{Compressor, CompressorConfig, Decompressor, TimestampMicros, TimestampNanos, BitWriter};
  use crate::types::NumberLike;

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
        TimestampNanos::from_secs_and_nanos(i64::MIN, 0),
        TimestampNanos::from_secs_and_nanos(i64::MAX, 999_999_999),
        TimestampNanos::from_secs_and_nanos(i64::MIN, 999_999_999),
        TimestampNanos::from_secs_and_nanos(0, 123_456_789),
        TimestampNanos::from_secs_and_nanos(-1, 123_456_789),
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

  #[test]
  fn test_multi_chunk() {
    let compressor = Compressor::<i64>::default();
    let mut writer = BitWriter::default();
    compressor.header(&mut writer).unwrap();
    compressor.compress_chunk(&[1, 2, 3], &mut writer).unwrap();
    compressor.compress_chunk(&[11, 12, 13], &mut writer).unwrap();
    compressor.footer(&mut writer).unwrap();
    let bytes = writer.pop();

    let decompressor = Decompressor::<i64>::default();
    let res = decompressor.simple_decompress(bytes).unwrap();
    assert_eq!(
      res,
      vec![1, 2, 3, 11, 12, 13],
    );
  }

  fn assert_recovers<T: NumberLike>(vals: Vec<T>, compression_level: u32) {
    for delta_encoding_order in [0, 1, 7] {
      let compressor = Compressor::<T>::from_config(
        CompressorConfig { compression_level, delta_encoding_order },
      );
      let compressed = compressor.simple_compress(&vals).expect("compression error");
      let decompressor = Decompressor::<T>::default();
      let decompressed = decompressor.simple_decompress(compressed)
        .expect("decompression error");
      // We can't do assert_eq on the whole vector because even bitwise identical
      // floats sometimes aren't equal by ==.
      assert_eq!(decompressed.len(), vals.len(), "on delta encoding order {}", delta_encoding_order);
      for i in 0..decompressed.len() {
        assert!(decompressed[i].num_eq(&vals[i]), "on delta encoding order {}", delta_encoding_order);
      }
    }
  }
}
