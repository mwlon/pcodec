use std::io::Write;
use crate::data_types::NumberLike;
use crate::{bits, CompressorConfig, DecompressorConfig};
use crate::errors::QCompressResult;
use crate::standalone::{Compressor, Decompressor};

const DEFAULT_CHUNK_SIZE: usize = 1_000_000;

/// Takes in a slice of numbers and returns compressed bytes.
///
/// Unlike most methods, this does not guarantee atomicity of the
/// compressor's state.
pub fn simple_compress<T: NumberLike>(config: CompressorConfig, nums: &[T]) -> Vec<u8> {
  // The following unwraps are safe because the writer will be byte-aligned
  // after each step and ensure each chunk has appropriate size.
  let mut compressor = Compressor::<T>::from_config(config);

  compressor.header().unwrap();

  if !nums.is_empty() {
    let n_chunks = bits::ceil_div(nums.len(), DEFAULT_CHUNK_SIZE);
    let n_per_chunk = bits::ceil_div(nums.len(), n_chunks);
    nums.chunks(n_per_chunk).for_each(|chunk| {
      compressor.chunk(chunk).unwrap();
    });
  }

  compressor.footer().unwrap();
  compressor.drain_bytes()
}

/// Takes in compressed bytes and returns a vector of numbers.
/// Will return an error if there are any compatibility, corruption,
/// or insufficient data issues.
///
/// Unlike most methods, this does not guarantee atomicity of the
/// compressor's state.
pub fn simple_decompress<T: NumberLike>(config: DecompressorConfig, bytes: &[u8]) -> QCompressResult<Vec<T>> {
  // cloning/extending by a single chunk's numbers can slow down by 2%
  // so we just take ownership of the first chunk's numbers instead
  let mut decompressor = Decompressor::<T>::from_config(config);
  decompressor.write_all(bytes).unwrap();
  let mut res: Option<Vec<T>> = None;
  decompressor.header()?;
  while decompressor.chunk_metadata()?.is_some() {
    let nums = decompressor.chunk_body()?;
    res = match res {
      Some(mut existing) => {
        existing.extend(nums);
        Some(existing)
      }
      None => Some(nums),
    };
  }
  Ok(res.unwrap_or_default())
}

