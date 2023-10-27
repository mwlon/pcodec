use crate::bit_reader::BitReader;
use crate::data_types::NumberLike;
use crate::errors::{PcoError, PcoResult};
use crate::progress::Progress;
use crate::standalone::constants::{
  BITS_TO_ENCODE_COMPRESSED_PAGE_SIZE, BITS_TO_ENCODE_N_ENTRIES, MAGIC_HEADER,
  MAGIC_TERMINATION_BYTE, STANDALONE_CHUNK_PREAMBLE_PADDING,
};
use crate::{bit_reader, wrapped, ChunkMetadata};

pub struct FileDecompressor(wrapped::FileDecompressor);

impl FileDecompressor {
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
    let consumed = reader.bytes_consumed()?;

    let (inner, additional) = wrapped::FileDecompressor::new(&src[consumed..])?;
    Ok((Self(inner), consumed + additional))
  }

  pub fn format_version(&self) -> u8 {
    self.0.format_version()
  }

  pub fn chunk_decompressor<T: NumberLike>(
    &self,
    src: &[u8],
  ) -> PcoResult<(Option<ChunkDecompressor<T>>, usize)> {
    let extension = bit_reader::make_extension_for(src, STANDALONE_CHUNK_PREAMBLE_PADDING);
    let mut reader = BitReader::new(src, &extension);
    let dtype_or_termination_byte = reader.read_aligned_bytes(1)?[0];

    if dtype_or_termination_byte == MAGIC_TERMINATION_BYTE {
      return Ok((None, reader.bytes_consumed()?));
    }

    if dtype_or_termination_byte != T::DTYPE_BYTE {
      return Err(PcoError::corruption(format!(
        "data type byte does not match {:?}; instead found {:?}",
        T::DTYPE_BYTE,
        dtype_or_termination_byte,
      )));
    }

    let n = reader.read_usize(BITS_TO_ENCODE_N_ENTRIES) + 1;
    let compressed_page_size = reader.read_usize(BITS_TO_ENCODE_COMPRESSED_PAGE_SIZE);
    let mut consumed = reader.bytes_consumed()?;
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
      compressed_page_size,
      n_bytes_processed: consumed - pre_page_consumed,
    };

    Ok((Some(res), consumed))
  }
}

pub struct ChunkDecompressor<T: NumberLike> {
  inner_cd: wrapped::ChunkDecompressor<T>,
  inner_pd: wrapped::PageDecompressor<T>,
  n: usize,
  n_processed: usize,
  compressed_page_size: usize,
  n_bytes_processed: usize,
}

impl<T: NumberLike> ChunkDecompressor<T> {
  pub fn metadata(&self) -> &ChunkMetadata<T::Unsigned> {
    &self.inner_cd.meta
  }

  pub fn n(&self) -> usize {
    self.n
  }

  pub fn compressed_body_size(&self) -> usize {
    self.compressed_page_size
  }

  pub fn decompress(&mut self, src: &[u8], dst: &mut [T]) -> PcoResult<(Progress, usize)> {
    let (progress, consumed) = self.inner_pd.decompress_sliced(src, dst)?;

    self.n_processed += progress.n_processed;
    self.n_bytes_processed += consumed;

    if self.n_processed >= self.n && self.n_bytes_processed != self.compressed_page_size {
      return Err(PcoError::corruption(format!(
        "Expected {} bytes in data page but read {} by the end",
        self.compressed_page_size, self.n_bytes_processed,
      )));
    } else if self.n_bytes_processed > self.compressed_page_size {
      return Err(PcoError::corruption(format!(
        "Expected {} bytes in data page but read {} before reaching the end",
        self.compressed_page_size, self.n_bytes_processed,
      )));
    }

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
