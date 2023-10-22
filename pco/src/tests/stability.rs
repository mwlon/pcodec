use crate::chunk_metadata::ChunkMetadata;
use crate::data_types::NumberLike;
use crate::errors::ErrorKind;
use crate::standalone::{auto_decompress};
use crate::chunk_config::ChunkConfig;

fn assert_panic_safe<T: NumberLike>(nums: Vec<T>) -> ChunkMetadata<T::Unsigned> {
  let mut compressor = Compressor::from_config(ChunkConfig {
    use_gcds: false,
    delta_encoding_order: Some(0),
    ..Default::default()
  })
  .unwrap();
  compressor.header().expect("header");
  let metadata = compressor.chunk(&nums).expect("chunk");
  compressor.footer().expect("footer");
  let compressed = compressor.drain_bytes();

  for i in 0..compressed.len() - 1 {
    match auto_decompress::<T>(&compressed[0..i]) {
      Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => (), // good
      Ok(_) => panic!("expected decompressor to notice insufficient data (got Ok)"),
      Err(e) => panic!(
        "expected decompressor to notice insufficient data (got {})",
        e
      ),
    }
  }

  metadata
}

#[test]
fn test_insufficient_data_short_bins() {
  let mut nums = Vec::new();
  for _ in 0..50 {
    nums.push(0);
  }
  for _ in 0..50 {
    nums.push(1000);
  }

  let metadata = assert_panic_safe(nums);
  assert_eq!(metadata.latents.len(), 1);
  assert_eq!(metadata.latents[0].bins.len(), 2);
}

#[test]
fn test_insufficient_data_sparse() {
  let mut nums = vec![0];
  for _ in 0..(1 << 16) + 1 {
    nums.push(1);
  }

  let metadata = assert_panic_safe(nums);
  assert_eq!(metadata.latents.len(), 1);
  assert_eq!(metadata.latents[0].bins.len(), 2);
}

#[test]
fn test_insufficient_data_long_offsets() {
  let n = 1000;
  let mut nums = Vec::new();
  for i in 0..n {
    nums.push((u64::MAX / n) * i);
  }

  let metadata = assert_panic_safe(nums);
  assert_eq!(metadata.latents.len(), 1);
  assert_eq!(metadata.latents[0].bins.len(), 1);
  assert_eq!(metadata.latents[0].bins[0].offset_bits, 64);
}
