use crate::bit_reader::BitReader;
use crate::data_types::NumberLike;
use crate::errors::{PcoError, PcoResult};
use crate::progress::Progress;
use crate::standalone::constants::{
  BITS_TO_ENCODE_N_ENTRIES, MAGIC_HEADER, MAGIC_TERMINATION_BYTE, STANDALONE_CHUNK_PREAMBLE_PADDING,
};
use crate::{bit_reader, wrapped, ChunkMeta};

/// Top-level entry point for decompressing standalone .pco files.
///
/// Example of the lowest level API for reading a .pco file:
/// ```
/// use pco::FULL_BATCH_SIZE;
/// use pco::standalone::FileDecompressor;
/// # use pco::errors::PcoResult;
///
/// # fn main() -> PcoResult<()> {
/// let src = vec![112, 99, 111, 33, 0, 0]; // the minimal .pco file, for the sake of example
/// let mut nums = vec![0; FULL_BATCH_SIZE];
/// let (file_decompressor, mut byte_idx) = FileDecompressor::new(&src)?;
/// let mut finished_file = false;
/// while !finished_file {
///   let (maybe_cd, bytes_read) = file_decompressor.chunk_decompressor::<i64>(
///     &src[byte_idx..]
///   )?;
///   byte_idx += bytes_read;
///   if let Some(mut chunk_decompressor) = maybe_cd {
///     let mut finished_chunk = false;
///     while !finished_chunk {
///       let (progress, bytes_read) = chunk_decompressor.decompress(
///         &src[byte_idx..],
///         &mut nums,
///       )?;
///       byte_idx += bytes_read;
///       // Do something with &nums[0..progress.n_processed]
///       finished_chunk = progress.finished_page;
///     }
///   } else {
///     finished_file = true;
///   }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct FileDecompressor(wrapped::FileDecompressor);

impl FileDecompressor {
  /// Reads a short header and returns a `FileDecompressor` and the number of
  /// bytes read.
  ///
  /// Will return an error if any corruptions, version incompatibilities, or
  /// insufficient data are found.
  pub fn new(src: &[u8]) -> PcoResult<(Self, usize)> {
    let extension = bit_reader::make_extension_for(src, MAGIC_HEADER.len());
    let mut reader = BitReader::new(src, &extension);
    let header = reader.read_aligned_bytes(MAGIC_HEADER.len())?;
    reader.check_in_bounds()?;
    if header != MAGIC_HEADER {
      return Err(PcoError::corruption(format!(
        "magic header does not match {:?}; instead found {:?}",
        MAGIC_HEADER, header,
      )));
    }
    let consumed = reader.aligned_bytes_consumed()?;

    let (inner, additional) = wrapped::FileDecompressor::new(&src[consumed..])?;
    Ok((Self(inner), consumed + additional))
  }

  pub fn format_version(&self) -> u8 {
    self.0.format_version()
  }

  /// Reads a chunk's metadata and returns a `ChunkDecompressor` and the
  /// number of bytes read.
  ///
  /// Will return None for the chunk decompressor if we've reached the footer,
  /// and will return an error if corruptions or insufficient
  /// data are found.
  pub fn chunk_decompressor<T: NumberLike>(
    &self,
    src: &[u8],
  ) -> PcoResult<(Option<ChunkDecompressor<T>>, usize)> {
    let extension = bit_reader::make_extension_for(src, STANDALONE_CHUNK_PREAMBLE_PADDING);
    let mut reader = BitReader::new(src, &extension);
    let dtype_or_termination_byte = reader.read_aligned_bytes(1)?[0];

    if dtype_or_termination_byte == MAGIC_TERMINATION_BYTE {
      return Ok((None, reader.aligned_bytes_consumed()?));
    }

    if dtype_or_termination_byte != T::DTYPE_BYTE {
      return Err(PcoError::corruption(format!(
        "data type byte does not match {:?}; instead found {:?}",
        T::DTYPE_BYTE,
        dtype_or_termination_byte,
      )));
    }

    let n = reader.read_usize(BITS_TO_ENCODE_N_ENTRIES) + 1;
    let mut consumed = reader.aligned_bytes_consumed()?;
    let (inner_cd, additional) = self.0.chunk_decompressor::<T>(&src[consumed..])?;
    consumed += additional;
    let pre_page_consumed = consumed;
    let (inner_pd, additional) = inner_cd.page_decompressor(n, &src[consumed..])?;
    consumed += additional;

    let res = ChunkDecompressor {
      inner_cd,
      inner_pd,
      n,
      n_processed: 0,
      n_bytes_processed: consumed - pre_page_consumed,
    };

    Ok((Some(res), consumed))
  }
}

/// Holds metadata about a chunk and supports decompression.
#[derive(Clone, Debug)]
pub struct ChunkDecompressor<T: NumberLike> {
  inner_cd: wrapped::ChunkDecompressor<T>,
  inner_pd: wrapped::PageDecompressor<T>,
  n: usize,
  n_processed: usize,
  n_bytes_processed: usize,
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
  /// the number of bytes read.
  ///
  /// Will return an error if corruptions or insufficient data are found.
  ///
  /// `dst` must have length either a multiple of 256 or be at least the count
  /// of numbers remaining in the chunk.
  pub fn decompress(&mut self, src: &[u8], dst: &mut [T]) -> PcoResult<(Progress, usize)> {
    let (progress, consumed) = self.inner_pd.decompress(src, dst)?;

    self.n_processed += progress.n_processed;
    self.n_bytes_processed += consumed;

    Ok((progress, consumed))
  }

  // a helper for some internal things
  pub(crate) fn decompress_remaining_extend(
    &mut self,
    bytes: &[u8],
    dst: &mut Vec<T>,
  ) -> PcoResult<usize> {
    let initial_len = dst.len();
    let remaining = self.n - self.n_processed;
    dst.reserve(remaining);
    unsafe {
      dst.set_len(initial_len + remaining);
    }
    let result = self.decompress(bytes, &mut dst[initial_len..]);
    if result.is_err() {
      dst.truncate(initial_len);
    }
    let (progress, consumed) = result?;
    assert!(progress.finished_page);
    Ok(consumed)
  }
}
