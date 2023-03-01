use crate::CompressorConfig;
use crate::data_types::NumberLike;
use crate::standalone::{auto_decompress, Compressor, simple_compress};

#[test]
fn test_edge_cases() {
  assert_recovers(vec![u64::MIN, u64::MAX], 0, "int extremes 0");
  assert_recovers(
    vec![f64::MIN, f64::MAX],
    0,
    "float extremes 0",
  );
  assert_recovers(vec![1.2_f32], 0, "float 0");
  assert_recovers(vec![1.2_f32], 1, "float 1");
  assert_recovers(vec![1.2_f32], 2, "float 2");
  assert_recovers(Vec::<u32>::new(), 6, "empty 6");
  assert_recovers(Vec::<u32>::new(), 0, "empty 0");
}

#[test]
fn test_moderate_data() {
  let mut v = Vec::new();
  for i in -50000..50000 {
    v.push(i);
  }
  assert_recovers(v, 5, "moderate data");
}

#[test]
fn test_sparse() {
  let mut v = Vec::new();
  for _ in 0..10000 {
    v.push(1);
  }
  v.push(0);
  v.push(0);
  v.push(1);
  assert_recovers(v, 1, "sparse");
}

#[test]
fn test_u32_codec() {
  assert_recovers(vec![0_u32, u32::MAX, 3, 4, 5], 1, "u32s");
}

#[test]
fn test_u64_codec() {
  assert_recovers(vec![0_u64, u64::MAX, 3, 4, 5], 1, "u64s");
}

#[test]
fn test_i32_codec() {
  assert_recovers(
    vec![0_i32, -1, i32::MAX, i32::MIN, 7],
    1,
    "i32s",
  );
}

#[test]
fn test_i64_codec() {
  assert_recovers(
    vec![0_i64, -1, i64::MAX, i64::MIN, 7],
    1,
    "i64s",
  );
}

#[test]
fn test_f32_codec() {
  assert_recovers(
    vec![
      f32::MAX,
      f32::MIN,
      f32::NAN,
      f32::NEG_INFINITY,
      f32::INFINITY,
      0.0,
      77.7,
    ],
    1,
    "f32s",
  );
}

#[test]
fn test_f64_codec() {
  assert_recovers(
    vec![
      f64::MAX,
      f64::MIN,
      f64::NAN,
      f64::NEG_INFINITY,
      f64::INFINITY,
      0.0,
      77.7,
    ],
    1,
    "f64s",
  );
}

#[test]
fn test_multi_chunk() {
  let mut compressor = Compressor::<i64>::default();
  compressor.header().unwrap();
  compressor.chunk(&[1, 2, 3]).unwrap();
  compressor.chunk(&[11, 12, 13]).unwrap();
  compressor.footer().unwrap();
  let bytes = compressor.drain_bytes();

  let res = auto_decompress::<i64>(&bytes).unwrap();
  assert_eq!(res, vec![1, 2, 3, 11, 12, 13], "multi chunk",);
}

#[test]
fn test_with_gcds() {
  assert_recovers(vec![7, 7, 21, 21], 1, "trivial gcd ranges");
  assert_recovers(
    vec![7, 7, 21, 28],
    1,
    "one trivial gcd range",
  );
  assert_recovers(
    vec![7, 14, 21, 28],
    1,
    "nontrivial gcd ranges",
  );
  assert_recovers(vec![7, 14, 22, 29], 1, "offset gcds");
  assert_recovers(
    vec![7, 11, 13, 17],
    1,
    "partially offset gcds",
  );

  let mut sparse_with_gcd = vec![15, 23, 31, 39];
  for _ in 0..100 {
    sparse_with_gcd.push(7);
  }
  assert_recovers(sparse_with_gcd, 4, "sparse with gcd");
}

fn assert_recovers<T: NumberLike>(nums: Vec<T>, compression_level: usize, name: &str) {
  for delta_encoding_order in [0, 1, 7] {
    for use_gcds in [false, true] {
      let debug_info = format!(
        "name={} delta_encoding_order={}, use_gcds={}",
        name, delta_encoding_order, use_gcds,
      );
      let config =
        CompressorConfig {
          compression_level,
          delta_encoding_order,
          use_gcds,
          ..Default::default()
        };
      let compressed = simple_compress(config, &nums);
      let decompressed = auto_decompress::<T>(&compressed).unwrap();
      // We can't do assert_eq on the whole vector because even bitwise identical
      // floats sometimes aren't equal by ==.
      assert_eq!(
        decompressed.len(),
        nums.len(),
        "{}",
        debug_info
      );
      for i in 0..decompressed.len() {
        // directly comparing numbers might not work for floats
        assert!(
          decompressed[i].to_unsigned() == nums[i].to_unsigned(),
          "{} != {} at {}; {}",
          decompressed[i],
          nums[i],
          i,
          debug_info,
        );
      }
    }
  }
}
