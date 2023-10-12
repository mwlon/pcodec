use std::io::Write;

use crate::data_types::NumberLike;
use crate::errors::PcoResult;
use crate::standalone::{Compressor, Decompressor};
use crate::{bits, CompressorConfig};
use crate::standalone::decompressor::FileDecompressor;

const DEFAULT_CHUNK_SIZE: usize = 1_000_000;

/// Takes in a slice of numbers and an exact configuration and returns
/// compressed bytes.
///
/// Will return an error if the compressor config is invalid.
pub fn simple_compress<T: NumberLike>(nums: &[T], config: CompressorConfig) -> PcoResult<Vec<u8>> {
  // The following unwraps are safe because the writer will be byte-aligned
  // after each step and ensure each chunk has appropriate size.
  let mut compressor = Compressor::<T>::from_config(config)?;

  compressor.header().unwrap();

  if !nums.is_empty() {
    let n_chunks = bits::ceil_div(nums.len(), DEFAULT_CHUNK_SIZE);
    let n_per_chunk = bits::ceil_div(nums.len(), n_chunks);
    nums.chunks(n_per_chunk).for_each(|chunk| {
      compressor.chunk(chunk).unwrap();
    });
  }

  compressor.footer().unwrap();
  Ok(compressor.drain_bytes())
}

/// Takes in compressed bytes and an exact configuration and returns a vector
/// of numbers.
///
/// Will return an error if there are any compatibility, corruption,
/// or insufficient data issues.
pub fn simple_decompress<T: NumberLike>(
  bytes: &[u8],
) -> PcoResult<Vec<T>> {
  // cloning/extending by a single chunk's numbers can slow down by 2%
  // so we just take ownership of the first chunk's numbers instead
  let (file_decompressor, mut bytes) = FileDecompressor::new(bytes)?;

  let mut res = Vec::new();
  let mut n = 0;
  while let (Some(mut chunk_decompressor), rest) = file_decompressor.chunk_decompressor(bytes)? {
    let meta = chunk_decompressor.metadata();
    res.reserve(meta.n);
    unsafe {
      res.set_len(n + meta.n);
    }
    let (progress, rest) = chunk_decompressor.decompress(rest, &mut res[n..])?;
    assert!(progress.finished_page);
    bytes = rest;
    n += meta.n;
  }
  Ok(res)
}

/// Automatically makes an educated guess for the best compression
/// configuration, based on `nums` and `compression_level`,
/// then uses [`simple_compress`] to compresses the numbers to .pco bytes.
///
/// This adds some compute cost by trying different configurations on a subset
/// of the numbers to determine the most likely one to do well.
/// If you know what configuration you want ahead of time (namely delta
/// encoding order), you can use [`Compressor::from_config`] instead to spare
/// the compute cost.
/// See [`CompressorConfig`] for information about compression levels.
pub fn auto_compress<T: NumberLike>(nums: &[T], compression_level: usize) -> Vec<u8> {
  let config = CompressorConfig {
    compression_level,
    ..Default::default()
  };
  simple_compress(nums, config).unwrap()
}

/// Automatically makes an educated guess for the best decompression
/// configuration, then uses [`simple_decompress`] to decompress .pco bytes
/// into numbers.
///
/// There are currently no relevant fields in the decompression configuration,
/// so there is no compute downside to using this function.
pub fn auto_decompress<T: NumberLike>(bytes: &[u8]) -> PcoResult<Vec<T>> {
  simple_decompress(DecompressorConfig::default(), bytes)
}
