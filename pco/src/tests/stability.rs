use crate::chunk_config::ChunkConfig;
use crate::chunk_metadata::ChunkMetadata;
use crate::data_types::NumberLike;
use crate::errors::{ErrorKind, PcoResult};
use crate::standalone::{auto_decompress, FileCompressor};

fn assert_panic_safe<T: NumberLike>(nums: Vec<T>) -> PcoResult<ChunkMetadata<T::Unsigned>> {
  let fc = FileCompressor::new();
  let config = ChunkConfig {
    use_gcds: false,
    delta_encoding_order: Some(0),
    ..Default::default()
  };
  let cc = fc.chunk_compressor(&nums, &config)?;
  let metadata = cc.chunk_meta().clone();
  let mut compressed =
    vec![0; fc.header_size_hint() + cc.chunk_size_hint() + fc.footer_size_hint()];
  let mut consumed = fc.write_header(&mut compressed)?;
  consumed += cc.write_chunk(&mut compressed[consumed..])?;
  consumed += fc.write_footer(&mut compressed[consumed..])?;
  compressed.truncate(consumed);

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

  Ok(metadata)
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

  let metadata = assert_panic_safe(nums)?;
  assert_eq!(metadata.latents.len(), 1);
  assert_eq!(metadata.latents[0].bins.len(), 2);
  Ok(())
}

#[test]
fn test_insufficient_data_sparse() -> PcoResult<()> {
  let mut nums = vec![0];
  for _ in 0..(1 << 16) + 1 {
    nums.push(1);
  }

  let metadata = assert_panic_safe(nums)?;
  assert_eq!(metadata.latents.len(), 1);
  assert_eq!(metadata.latents[0].bins.len(), 2);
  Ok(())
}

#[test]
fn test_insufficient_data_long_offsets() -> PcoResult<()> {
  let n = 1000;
  let mut nums = Vec::new();
  for i in 0..n {
    nums.push((u64::MAX / n) * i);
  }

  let metadata = assert_panic_safe(nums)?;
  assert_eq!(metadata.latents.len(), 1);
  assert_eq!(metadata.latents[0].bins.len(), 1);
  assert_eq!(metadata.latents[0].bins[0].offset_bits, 64);
  Ok(())
}
