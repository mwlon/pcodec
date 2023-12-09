use crate::chunk_config::ChunkConfig;
use crate::data_types::NumberLike;
use crate::errors::PcoResult;
use crate::progress::Progress;
use crate::standalone::compressor::FileCompressor;
use crate::standalone::decompressor::{FileDecompressor, MaybeChunkDecompressor};
use crate::{PagingSpec, FULL_BATCH_N};
use std::cmp::min;

/// Takes in a slice of numbers and an exact configuration and returns
/// compressed bytes.
///
/// Will return an error if the compressor config is invalid.
/// This will use the `PagingSpec` in `ChunkConfig` to decide where to split
/// chunks.
/// For standalone, the concepts of chunk and page are conflated since each
/// chunk has exactly one page.
pub fn simple_compress<T: NumberLike>(nums: &[T], config: &ChunkConfig) -> PcoResult<Vec<u8>> {
  let mut dst = Vec::new();
  let file_compressor = FileCompressor::default().with_n_hint(nums.len());
  file_compressor.write_header(&mut dst)?;

  // here we use the paging spec to determine chunks; each chunk has 1 page
  let n_per_page = config.paging_spec.n_per_page(nums.len())?;
  let mut start = 0;
  let mut this_chunk_config = config.clone();
  let mut hinted_size = false;
  for &page_n in &n_per_page {
    let end = start + page_n;
    this_chunk_config.paging_spec = PagingSpec::ExactPageSizes(vec![page_n]);
    let chunk_compressor =
      file_compressor.chunk_compressor(&nums[start..end], &this_chunk_config)?;

    if !hinted_size {
      let file_size_hint =
        chunk_compressor.chunk_size_hint() as f64 * nums.len() as f64 / page_n as f64;
      dst.reserve_exact(file_size_hint as usize + 10);
      hinted_size = true;
    }

    chunk_compressor.write_chunk(&mut dst)?;
    start = end;
  }

  file_compressor.write_footer(&mut dst)?;
  Ok(dst)
}

/// Takes in compressed bytes and writes numbers to the destination, returning
/// progress into the file.
///
/// Will return an error if there are any compatibility, corruption,
/// or insufficient data issues.
/// Does not error if dst is too short or too long, but that can be inferred
/// from `Progress`.
pub fn simple_decompress_into<T: NumberLike>(src: &[u8], mut dst: &mut [T]) -> PcoResult<Progress> {
  let (file_decompressor, mut src) = FileDecompressor::new(src)?;

  let mut incomplete_batch_buffer = vec![T::default(); FULL_BATCH_N];
  let mut progress = Progress::default();
  loop {
    let maybe_cd = file_decompressor.chunk_decompressor(src)?;
    let mut chunk_decompressor;
    match maybe_cd {
      MaybeChunkDecompressor::Some(cd) => chunk_decompressor = cd,
      MaybeChunkDecompressor::EndOfData(_) => {
        progress.finished = true;
        break;
      }
    }

    let (limit, is_limited) = if dst.len() < chunk_decompressor.n() {
      (dst.len() / FULL_BATCH_N * FULL_BATCH_N, true)
    } else {
      (dst.len(), false)
    };

    let new_progress = chunk_decompressor.decompress(&mut dst[..limit])?;
    dst = &mut dst[new_progress.n_processed..];
    progress.n_processed += new_progress.n_processed;

    // If we're near the end of dst, we do one possibly incomplete batch
    // of numbers and copy them over.
    if !dst.is_empty() {
      let new_progress = chunk_decompressor.decompress(&mut incomplete_batch_buffer)?;
      let n_processed = min(dst.len(), new_progress.n_processed);
      dst[..n_processed].copy_from_slice(&incomplete_batch_buffer[..n_processed]);
      dst = &mut dst[n_processed..];
      progress.n_processed += n_processed;
    }

    if dst.is_empty() && is_limited {
      break;
    }

    src = chunk_decompressor.into_src();
  }
  Ok(progress)
}

// TODO in 0.2 make this return an error instead of panic
/// Compresses the numbers using the given compression level and an otherwise
/// default configuration.
///
/// Will panic if the compression level is invalid (see
/// [`ChunkConfig`][crate::ChunkConfig] for an explanation of compression
/// levels).
/// This wraps [`simple_compress`].
pub fn auto_compress<T: NumberLike>(nums: &[T], compression_level: usize) -> Vec<u8> {
  let config = ChunkConfig {
    compression_level,
    ..Default::default()
  };
  simple_compress(nums, &config).unwrap()
}

/// Takes in compressed bytes and returns a vector of numbers.
///
/// Will return an error if there are any compatibility, corruption,
/// or insufficient data issues.
pub fn auto_decompress<T: NumberLike>(src: &[u8]) -> PcoResult<Vec<T>> {
  let (file_decompressor, mut src) = FileDecompressor::new(src)?;

  let mut res = Vec::with_capacity(file_decompressor.n_hint());
  while let MaybeChunkDecompressor::Some(mut chunk_decompressor) =
    file_decompressor.chunk_decompressor(src)?
  {
    chunk_decompressor.decompress_remaining_extend(&mut res)?;
    src = chunk_decompressor.into_src();
  }
  Ok(res)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_simple_decompress_into() -> PcoResult<()> {
    let max_n = 600;
    let nums = (0..max_n).map(|x| x as i32).collect::<Vec<i32>>();
    let src = simple_compress(
      &nums,
      &ChunkConfig {
        compression_level: 0,
        delta_encoding_order: Some(0),
        paging_spec: PagingSpec::ExactPageSizes(vec![300, 300]),
        ..Default::default()
      },
    )?;

    for possibly_overshooting_n in [0, 1, 256, 299, 300, 301, 556, 600, 601] {
      let mut dst = vec![0; possibly_overshooting_n];
      let progress = simple_decompress_into(&src, &mut dst)?;
      let n = min(possibly_overshooting_n, max_n);
      assert_eq!(progress.n_processed, n);
      assert_eq!(progress.finished, n >= nums.len());
      assert_eq!(
        &dst[..n],
        &(0..n).map(|x| x as i32).collect::<Vec<i32>>(),
        "n={}",
        n
      );
    }

    Ok(())
  }
}
