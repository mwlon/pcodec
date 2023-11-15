use crate::chunk_config::ChunkConfig;
use crate::data_types::NumberLike;
use crate::errors::PcoResult;
use crate::standalone::compressor::FileCompressor;
use crate::standalone::decompressor::{FileDecompressor, MaybeChunkDecompressor};
use crate::PagingSpec;

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
  let file_compressor = FileCompressor::default();
  file_compressor.write_header(&mut dst)?;

  // here we use the paging spec to determine chunks; each chunk has 1 page
  let n_per_page = config.paging_spec.n_per_page(nums.len())?;
  let mut start = 0;
  let mut this_chunk_config = config.clone();
  for &page_n in &n_per_page {
    let end = start + page_n;
    this_chunk_config.paging_spec = PagingSpec::ExactPageSizes(vec![page_n]);
    let chunk_compressor =
      file_compressor.chunk_compressor(&nums[start..end], &this_chunk_config)?;
    dst.reserve(chunk_compressor.chunk_size_hint());
    chunk_compressor.write_chunk(&mut dst)?;
    start = end;
  }

  file_compressor.write_footer(&mut dst)?;
  Ok(dst)
}

/// Automatically makes an educated guess for the best compression
/// configuration, based on `nums` and `compression_level`,
/// then uses [`simple_compress`] to compresses the numbers to .pco bytes.
///
/// This adds some compute cost by trying different configurations on a subset
/// of the numbers to determine the most likely one to do well.
/// If you know what configuration you want ahead of time (namely delta
/// encoding order), you can use [`simple_compress`] instead to spare
/// the compute cost.
/// See [`ChunkConfig`] for information about compression levels.
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

  let mut res = Vec::new();
  while let MaybeChunkDecompressor::Some(mut chunk_decompressor) =
    file_decompressor.chunk_decompressor(src)?
  {
    chunk_decompressor.decompress_remaining_extend(&mut res)?;
    src = chunk_decompressor.into_src();
  }
  Ok(res)
}
