use better_io::BetterBufRead;

use crate::bit_reader::BitReaderBuilder;
use crate::data_types::NumberLike;
use crate::errors::{PcoError, PcoResult};
use crate::progress::Progress;
use crate::standalone::constants::{
  BITS_TO_ENCODE_N_ENTRIES, MAGIC_HEADER, MAGIC_TERMINATION_BYTE, STANDALONE_CHUNK_PREAMBLE_PADDING,
};
use crate::{wrapped, ChunkMeta};

/// Top-level entry point for decompressing standalone .pco files.
///
/// Example of the lowest level API for reading a .pco file:
/// ```
/// use pco::FULL_BATCH_N;
/// use pco::standalone::FileDecompressor;
/// # use pco::errors::PcoResult;
///
/// # fn main() -> PcoResult<()> {
/// let compressed = vec![112, 99, 111, 33, 0, 0]; // the minimal .pco file, for the sake of example
/// let mut nums = vec![0; FULL_BATCH_N];
/// let (file_decompressor, mut src) = FileDecompressor::new(compressed.as_slice())?;
/// while let (Some(mut chunk_decompressor), new_src) = file_decompressor.chunk_decompressor::<i64, _>(src)? {
///   src = new_src;
///   let mut finished_chunk = false;
///   while !finished_chunk {
///     let (progress, new_src) = chunk_decompressor.decompress(
///       src,
///       &mut nums,
///     )?;
///     src = new_src;
///     // Do something with &nums[0..progress.n_processed]
///     finished_chunk = progress.finished_page;
///   }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct FileDecompressor(wrapped::FileDecompressor);

impl FileDecompressor {
  /// Reads a short header and returns a `FileDecompressor` and the
  /// remaining input.
  ///
  /// Will return an error if any corruptions, version incompatibilities, or
  /// insufficient data are found.
  pub fn new<R: BetterBufRead>(src: R) -> PcoResult<(Self, R)> {
    let mut reader_builder = BitReaderBuilder::new(src, MAGIC_HEADER.len(), 0);
    let header = reader_builder
      .with_reader(|reader| Ok(reader.read_aligned_bytes(MAGIC_HEADER.len())?.to_vec()))?;
    if header != MAGIC_HEADER {
      return Err(PcoError::corruption(format!(
        "magic header does not match {:?}; instead found {:?}",
        MAGIC_HEADER, header,
      )));
    }

    let (inner, rest) = wrapped::FileDecompressor::new(reader_builder.into_inner())?;
    Ok((Self(inner), rest))
  }

  pub fn format_version(&self) -> u8 {
    self.0.format_version()
  }

  /// Reads a chunk's metadata and returns a `ChunkDecompressor` and the
  /// remaining input.
  ///
  /// Will return None for the chunk decompressor if we've reached the footer,
  /// and will return an error if corruptions or insufficient
  /// data are found.
  pub fn chunk_decompressor<T: NumberLike, R: BetterBufRead>(
    &self,
    src: R,
  ) -> PcoResult<(Option<ChunkDecompressor<T>>, R)> {
    let mut reader_builder = BitReaderBuilder::new(src, STANDALONE_CHUNK_PREAMBLE_PADDING, 0);
    let dtype_or_termination_byte =
      reader_builder.with_reader(|reader| Ok(reader.read_aligned_bytes(1)?[0]))?;
    if dtype_or_termination_byte == MAGIC_TERMINATION_BYTE {
      return Ok((None, reader_builder.into_inner()));
    }

    if dtype_or_termination_byte != T::DTYPE_BYTE {
      return Err(PcoError::corruption(format!(
        "data type byte does not match {:?}; instead found {:?}",
        T::DTYPE_BYTE,
        dtype_or_termination_byte,
      )));
    }

    let n =
      reader_builder.with_reader(|reader| Ok(reader.read_usize(BITS_TO_ENCODE_N_ENTRIES) + 1))?;
    let src = reader_builder.into_inner();
    let (inner_cd, src) = self.0.chunk_decompressor::<T, R>(src)?;
    let (inner_pd, src) = inner_cd.page_decompressor(n, src)?;

    let res = ChunkDecompressor {
      inner_cd,
      inner_pd,
      n,
      n_processed: 0,
    };

    Ok((Some(res), src))
  }
}

/// Holds metadata about a chunk and supports decompression.
#[derive(Clone, Debug)]
pub struct ChunkDecompressor<T: NumberLike> {
  inner_cd: wrapped::ChunkDecompressor<T>,
  inner_pd: wrapped::PageDecompressor<T>,
  n: usize,
  n_processed: usize,
}

impl<T: NumberLike> ChunkDecompressor<T> {
  /// Returns pre-computed information about the chunk.
  pub fn meta(&self) -> &ChunkMeta<T::Unsigned> {
    &self.inner_cd.meta
  }

  /// Returns the count of numbers in the chunk.
  pub fn n(&self) -> usize {
    self.n
  }

  /// Reads compressed numbers into the destination, returning progress and
  /// the remaining input.
  ///
  /// Will return an error if corruptions or insufficient data are found.
  ///
  /// `dst` must have length either a multiple of 256 or be at least the count
  /// of numbers remaining in the chunk.
  pub fn decompress<R: BetterBufRead>(
    &mut self,
    src: R,
    dst: &mut [T],
  ) -> PcoResult<(Progress, R)> {
    let (progress, src) = self.inner_pd.decompress(src, dst)?;

    self.n_processed += progress.n_processed;

    Ok((progress, src))
  }

  // a helper for some internal things
  pub(crate) fn decompress_remaining_extend<R: BetterBufRead>(
    &mut self,
    src: R,
    dst: &mut Vec<T>,
  ) -> PcoResult<R> {
    let initial_len = dst.len();
    let remaining = self.n - self.n_processed;
    dst.reserve(remaining);
    unsafe {
      dst.set_len(initial_len + remaining);
    }
    let result = self.decompress(src, &mut dst[initial_len..]);
    if result.is_err() {
      dst.truncate(initial_len);
    }
    let (progress, rest) = result?;
    assert!(progress.finished_page);
    Ok(rest)
  }
}
