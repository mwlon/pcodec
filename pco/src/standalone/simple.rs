use std::io::Write;

use crate::data_types::NumberLike;
use crate::errors::PcoResult;
use crate::bits;
use crate::chunk_config::ChunkConfig;
use crate::standalone::compressor::FileCompressor;
use crate::standalone::decompressor::FileDecompressor;

const DEFAULT_CHUNK_SIZE: usize = 1_000_000;

fn zero_pad(bytes: &mut Vec<u8>, additional: usize) {
  bytes.reserve(additional);
  for _ in 0..additional {
    bytes.push(0);
  }
}

/// Takes in a slice of numbers and an exact configuration and returns
/// compressed bytes.
///
/// Will return an error if the compressor config is invalid.
pub fn simple_compress<T: NumberLike>(nums: &[T], config: &ChunkConfig) -> PcoResult<Vec<u8>> {
  let mut bytes = Vec::new();
  let file_compressor = FileCompressor::new();
  zero_pad(&mut bytes, file_compressor.header_size_hint());
  let mut zeros_remaining = file_compressor.write_header(&mut bytes)?.len();

  let n_chunks = bits::ceil_div(nums.len(), DEFAULT_CHUNK_SIZE);
  let n_per_chunk = bits::ceil_div(nums.len(), n_chunks);
  for chunk in nums.chunks(n_per_chunk) {
    let chunk_compressor = file_compressor.chunk_compressor(chunk, &config)?;
    zero_pad(&mut bytes, chunk_compressor.chunk_size_hint().saturating_sub(zeros_remaining));
    zeros_remaining = chunk_compressor.write_chunk(&mut bytes)?.len();
  };

  zero_pad(&mut bytes, file_compressor.footer_size_hint().saturating_sub(zeros_remaining));
  zeros_remaining = file_compressor.write_footer(&mut bytes)?.len();

  bytes.truncate(bytes.len() - zeros_remaining);
  Ok(bytes)
}

/// Takes in compressed bytes and an exact configuration and returns a vector
/// of numbers.
///
/// Will return an error if there are any compatibility, corruption,
/// or insufficient data issues.
fn simple_decompress<T: NumberLike>(
  bytes: &[u8],
) -> PcoResult<Vec<T>> {
  let (file_decompressor, mut data) = FileDecompressor::new(bytes)?;

  let mut res = Vec::new();
  while let (Some(mut chunk_decompressor), rest) = file_decompressor.chunk_decompressor(data)? {
    data = chunk_decompressor.decompress_remaining_extend(rest, &mut res)?;
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
/// See [`ChunkConfig`] for information about compression levels.
pub fn auto_compress<T: NumberLike>(nums: &[T], compression_level: usize) -> Vec<u8> {
  let config = ChunkConfig {
    compression_level,
    ..Default::default()
  };
  simple_compress(nums, &config).unwrap()
}

/// Automatically makes an educated guess for the best decompression
/// configuration, then uses [`simple_decompress`] to decompress .pco bytes
/// into numbers.
///
/// There are currently no relevant fields in the decompression configuration,
/// so there is no compute downside to using this function.
pub fn auto_decompress<T: NumberLike>(bytes: &[u8]) -> PcoResult<Vec<T>> {
  simple_decompress(bytes)
}
