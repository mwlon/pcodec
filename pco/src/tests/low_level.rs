use std::io::Write;

use crate::data_types::NumberLike;
use crate::errors::{ErrorKind, PcoResult};
use crate::standalone::{FileDecompressor};
use crate::chunk_config::ChunkConfig;
use crate::constants::{CURRENT_FORMAT_VERSION, FULL_BATCH_SIZE};

#[test]
fn test_low_level_short() -> PcoResult<()> {
  let nums = vec![vec![0], vec![10, 11], vec![20, 21, 22]];
  assert_lowest_level_behavior(nums)
}

#[test]
fn test_low_level_long() -> PcoResult<()> {
  let nums = vec![(0..777).collect::<Vec<_>>()];
  assert_lowest_level_behavior(nums)
}

#[test]
fn test_low_level_sparse() -> PcoResult<()> {
  let mut nums = vec![0; 1000];
  nums.push(1);
  nums.resize(2000, 0);
  assert_lowest_level_behavior(vec![nums])
}

fn assert_lowest_level_behavior<T: NumberLike>(chunks: Vec<Vec<T>>) -> PcoResult<()> {
  for delta_encoding_order in [0, 7] {
    let debug_info = format!("delta order={}", delta_encoding_order);
    let mut compressor = Compressor::<T>::from_config(ChunkConfig {
      delta_encoding_order: Some(delta_encoding_order),
      ..Default::default()
    })
    .unwrap();
    compressor.header()?;
    let mut metadatas = Vec::new();
    for nums in &chunks {
      metadatas.push(compressor.chunk(nums)?);
    }
    compressor.footer()?;

    let bytes = compressor.drain_bytes();

    let (fd, mut bytes) = FileDecompressor::new(&bytes)?;
    assert_eq!(fd.format_version(), CURRENT_FORMAT_VERSION, "{}", debug_info);

    let mut chunk_idx = 0;
    let mut buffer = vec![T::default(); FULL_BATCH_SIZE];
    while let (Some(mut chunk_decompressor), rest) = fd.chunk_decompressor(bytes)? {
      let mut chunk_nums = Vec::<T>::new();
      bytes = rest;
      loop {
        let (progress, rest) = chunk_decompressor.decompress(bytes, &mut buffer)?;
        chunk_nums.extend(&buffer[..progress.n_processed]);
        bytes = rest;
        if progress.finished_page {
          break;
        }
      }
      assert_eq!(&chunk_nums, &chunks[chunk_idx]);

      chunk_idx += 1;
    }
  }
  Ok(())
}
