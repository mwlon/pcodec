use rand::Rng;

use crate::data_types::NumberLike;
use crate::errors::PcoResult;
use crate::standalone::{auto_decompress, FileCompressor, simple_compress};
use crate::chunk_config::ChunkConfig;

#[test]
fn test_edge_cases() -> PcoResult<()> {
  assert_recovers(vec![u64::MIN, u64::MAX], 0, "int extremes 0")?;
  assert_recovers(
    vec![f64::MIN, f64::MAX],
    0,
    "float extremes 0",
  )?;
  assert_recovers(vec![1.2_f32], 0, "float 0")?;
  assert_recovers(vec![1.2_f32], 1, "float 1")?;
  assert_recovers(vec![1.2_f32], 2, "float 2")?;
  assert_recovers(Vec::<u32>::new(), 6, "empty 6")?;
  assert_recovers(Vec::<u32>::new(), 0, "empty 0")
}

#[test]
fn test_moderate_data() -> PcoResult<()> {
  let mut v = Vec::new();
  for i in -50000..50000 {
    v.push(i);
  }
  assert_recovers(v, 3, "moderate data")
}

#[test]
fn test_sparse() -> PcoResult<()> {
  let mut v = Vec::new();
  for _ in 0..10000 {
    v.push(1);
  }
  v.push(0);
  v.push(0);
  v.push(1);
  assert_recovers(v, 1, "sparse")
}

#[test]
fn test_u32_codec() -> PcoResult<()> {
  assert_recovers(vec![0_u32, u32::MAX, 3, 4, 5], 1, "u32s")
}

#[test]
fn test_u64_codec() -> PcoResult<()> {
  assert_recovers(vec![0_u64, u64::MAX, 3, 4, 5], 1, "u64s")
}

#[test]
fn test_i32_codec() -> PcoResult<()> {
  assert_recovers(
    vec![0_i32, -1, i32::MAX, i32::MIN, 7],
    1,
    "i32s",
  )
}

#[test]
fn test_i64_codec() -> PcoResult<()> {
  assert_recovers(
    vec![0_i64, -1, i64::MAX, i64::MIN, 7],
    1,
    "i64s",
  )
}

#[test]
fn test_f32_codec() -> PcoResult<()> {
  assert_recovers(
    vec![
      f32::MAX,
      f32::MIN,
      f32::NAN,
      f32::NEG_INFINITY,
      f32::INFINITY,
      -0.0,
      0.0,
      77.7,
    ],
    1,
    "f32s",
  )
}

#[test]
fn test_f64_codec() -> PcoResult<()> {
  assert_recovers(
    vec![
      f64::MAX,
      f64::MIN,
      f64::NAN,
      f64::NEG_INFINITY,
      f64::INFINITY,
      -0.0,
      0.0,
      77.7,
    ],
    1,
    "f64s",
  )
}

#[test]
fn test_multi_chunk() -> PcoResult<()> {
  let config = ChunkConfig::default();
  let fc = FileCompressor::new();
  let mut bytes = vec![0; 300];
  let dst = &mut bytes;
  let dst = fc.write_header(dst)?;
  let dst = fc.chunk_compressor(&[1, 2, 3], &config)?.write_chunk(dst)?;
  let dst = fc.chunk_compressor(&[11, 12, 13], &config)?.write_chunk(dst)?;
  let dst = fc.write_footer(dst)?;
  bytes.truncate(bytes.len() - dst.len());

  let res = auto_decompress::<i64>(&bytes)?;
  assert_eq!(res, vec![1, 2, 3, 11, 12, 13], "multi chunk");
  Ok(())
}

#[test]
fn test_with_gcds() -> PcoResult<()> {
  assert_recovers(vec![7, 7, 21, 21], 1, "trivial gcd ranges")?;
  assert_recovers(
    vec![7, 7, 21, 28],
    1,
    "one trivial gcd range",
  )?;
  assert_recovers(
    vec![7, 14, 21, 28],
    1,
    "nontrivial gcd ranges",
  )?;
  assert_recovers(vec![7, 14, 22, 29], 1, "offset gcds")?;
  assert_recovers(
    vec![7, 11, 13, 17],
    1,
    "partially offset gcds",
  )?;

  let mut sparse_with_gcd = vec![15, 23, 31, 39];
  for _ in 0..100 {
    sparse_with_gcd.push(7);
  }
  assert_recovers(sparse_with_gcd, 4, "sparse with gcd")
}

#[test]
fn test_sparse_islands() -> PcoResult<()> {
  let mut rng = rand::thread_rng();
  let mut nums = Vec::new();
  // sparse - one common island of [0, 8) and one rare of [1000, 1008)
  for _ in 0..20 {
    for _ in 0..99 {
      nums.push(rng.gen_range(0..8))
    }
    nums.push(rng.gen_range(1000..1008))
  }
  assert_recovers(nums, 4, "sparse islands")
}

#[test]
fn test_decimals() -> PcoResult<()> {
  let mut rng = rand::thread_rng();
  let mut nums = Vec::new();
  let n = 300;

  pub fn plus_epsilons(a: f64, epsilons: i64) -> f64 {
    f64::from_unsigned(a.to_unsigned().wrapping_add(epsilons as u64))
  }

  for _ in 0..n {
    let unadjusted_num = (rng.gen_range(-1..100) as f64) * 0.01;
    let adj = rng.gen_range(-1..2);
    nums.push(plus_epsilons(unadjusted_num, adj));
  }
  // add some big numbers just to test losslessness
  nums.resize(2 * n, f64::INFINITY);
  // Each regular number should take only 7 bits for offset and 2 bits for
  // adjustment, plus some overhead. Each infinity should take 1 bit plus maybe
  // 2 for adjustment.
  let overhead_bytes = 100;
  assert_recovers_within_size(
    &nums,
    2,
    "decimals",
    0,
    (9 * n + 3 * n) / 8 + overhead_bytes,
  )?;
  assert_recovers(nums, 2, "decimals")
}

fn assert_recovers<T: NumberLike>(
  nums: Vec<T>,
  compression_level: usize,
  name: &str,
) -> PcoResult<()> {
  for delta_encoding_order in [0, 1, 7] {
    assert_recovers_within_size(
      &nums,
      compression_level,
      name,
      delta_encoding_order,
      usize::MAX,
    )?;
  }
  Ok(())
}

fn assert_recovers_within_size<T: NumberLike>(
  nums: &[T],
  compression_level: usize,
  name: &str,
  delta_encoding_order: usize,
  max_byte_size: usize,
) -> PcoResult<()> {
  let debug_info = format!(
    "name={} delta_encoding_order={}",
    name, delta_encoding_order,
  );
  let config = ChunkConfig {
    compression_level,
    delta_encoding_order: Some(delta_encoding_order),
    ..Default::default()
  };
  let compressed = simple_compress(nums, &config)?;
  assert!(
    compressed.len() <= max_byte_size,
    "compressed size {} > {}; {}",
    compressed.len(),
    max_byte_size,
    debug_info
  );
  let decompressed = auto_decompress::<T>(&compressed)?;
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
  Ok(())
}
