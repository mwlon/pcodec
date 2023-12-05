use crate::chunk_config::ChunkConfig;
use crate::chunk_meta::ChunkMeta;
use crate::data_types::NumberLike;
use crate::errors::{ErrorKind, PcoResult};
use crate::standalone::{auto_decompress, FileCompressor};
use crate::IntMultSpec;

fn assert_panic_safe<T: NumberLike>(nums: Vec<T>) -> PcoResult<ChunkMeta<T::Unsigned>> {
  let fc = FileCompressor::default();
  let config = ChunkConfig {
    int_mult_spec: IntMultSpec::Disabled,
    delta_encoding_order: Some(0),
    ..Default::default()
  };
  let cc = fc.chunk_compressor(&nums, &config)?;
  let meta = cc.meta().clone();
  let mut compressed = Vec::new();
  fc.write_header(&mut compressed)?;
  cc.write_chunk(&mut compressed)?;
  fc.write_footer(&mut compressed)?;

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

  Ok(meta)
}

#[test]
fn test_insufficient_data_short_bins() -> PcoResult<()> {
  let mut nums = Vec::new();
  for _ in 0..50 {
    nums.push(0);
  }
  for _ in 0..50 {
    nums.push(1000);
  }

  let meta = assert_panic_safe(nums)?;
  assert_eq!(meta.per_latent_var.len(), 1);
  assert_eq!(meta.per_latent_var[0].bins.len(), 2);
  Ok(())
}

#[test]
fn test_insufficient_data_sparse() -> PcoResult<()> {
  let mut nums = vec![0];
  for _ in 0..(1 << 16) + 1 {
    nums.push(1);
  }

  let meta = assert_panic_safe(nums)?;
  assert_eq!(meta.per_latent_var.len(), 1);
  assert_eq!(meta.per_latent_var[0].bins.len(), 2);
  Ok(())
}

#[test]
fn test_insufficient_data_long_offsets() -> PcoResult<()> {
  let n = 1000;
  let mut nums = Vec::new();
  for i in 0..n {
    nums.push((u64::MAX / n) * i);
  }

  let meta = assert_panic_safe(nums)?;
  assert_eq!(meta.per_latent_var.len(), 1);
  assert_eq!(meta.per_latent_var[0].bins.len(), 1);
  assert_eq!(
    meta.per_latent_var[0].bins[0].offset_bits,
    64
  );
  Ok(())
}
