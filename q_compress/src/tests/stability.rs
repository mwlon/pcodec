use crate::chunk_metadata::{ChunkMetadata, PrefixMetadata};
use crate::data_types::NumberLike;
use crate::errors::ErrorKind;
use crate::{Compressor, CompressorConfig};

fn assert_panic_safe<T: NumberLike>(nums: Vec<T>) -> ChunkMetadata<T> {
  let mut compressor = Compressor::from_config(CompressorConfig::default().with_use_gcds(false));
  compressor.header().expect("header");
  let metadata = compressor.chunk(&nums).expect("chunk");
  compressor.footer().expect("footer");
  let compressed = compressor.drain_bytes();

  for i in 0..compressed.len() - 1 {
    match crate::auto_decompress::<T>(&compressed[0..i]) {
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
fn test_insufficient_data_short_prefixes() {
  let mut nums = Vec::new();
  for _ in 0..50 {
    nums.push(0);
  }
  for _ in 0..50 {
    nums.push(1000);
  }

  let metadata = assert_panic_safe(nums);
  match metadata.prefix_metadata {
    PrefixMetadata::Simple { prefixes } => {
      assert_eq!(prefixes.len(), 2);
      for p in &prefixes {
        assert_eq!(p.code.len(), 1);
      }
    }
    _ => panic!("expected simple prefix info"),
  }
}

#[test]
fn test_insufficient_data_many_reps() {
  let mut nums = vec![false];
  for _ in 0..(1 << 16) + 1 {
    nums.push(true);
  }

  let metadata = assert_panic_safe(nums);
  match metadata.prefix_metadata {
    PrefixMetadata::Simple { prefixes } => {
      assert_eq!(prefixes.len(), 2);
      let has_reps = prefixes.iter().any(|p| p.run_len_jumpstart.is_some());
      if !has_reps {
        panic!("expected a prefix to have reps");
      }
    }
    _ => panic!("expected simple prefix info"),
  }
}

#[test]
fn test_insufficient_data_long_offsets() {
  let n = 1000;
  let mut nums = Vec::new();
  for i in 0..n {
    nums.push((u64::MAX / n) * i);
  }

  let metadata = assert_panic_safe(nums);
  match metadata.prefix_metadata {
    PrefixMetadata::Simple { prefixes } => {
      assert_eq!(prefixes.len(), 1);
      assert_eq!(prefixes[0].k_info().k, 63);
    }
    _ => panic!("expected simple prefix info"),
  }
}
