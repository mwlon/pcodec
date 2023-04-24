use crate::chunk_metadata::{BinMetadata, ChunkMetadata};
use crate::data_types::NumberLike;
use crate::errors::ErrorKind;
use crate::standalone::{auto_decompress, Compressor};
use crate::CompressorConfig;

fn assert_panic_safe<T: NumberLike>(nums: Vec<T>) -> ChunkMetadata<T> {
  let mut compressor = Compressor::from_config(CompressorConfig {
    use_gcds: false,
    ..Default::default()
  });
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
  match metadata.bin_metadata {
    BinMetadata::Simple { bins } => {
      assert_eq!(bins.len(), 2);
      for p in &bins {
        assert_eq!(p.code.len(), 1);
      }
    }
    _ => panic!("expected simple bin info"),
  }
}

#[test]
fn test_insufficient_data_many_reps() {
  let mut nums = vec![0];
  for _ in 0..(1 << 16) + 1 {
    nums.push(1);
  }

  let metadata = assert_panic_safe(nums);
  match metadata.bin_metadata {
    BinMetadata::Simple { bins } => {
      assert_eq!(bins.len(), 2);
      let has_reps = bins.iter().any(|p| p.run_len_jumpstart.is_some());
      if !has_reps {
        panic!("expected a bin to have reps");
      }
    }
    _ => panic!("expected simple bin info"),
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
  match metadata.bin_metadata {
    BinMetadata::Simple { bins } => {
      assert_eq!(bins.len(), 1);
      assert_eq!(bins[0].k_info(), 64);
    }
    _ => panic!("expected simple bin info"),
  }
}
