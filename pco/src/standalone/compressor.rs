use crate::bit_writer::BitWriter;
use crate::chunk_config::PagingSpec;
use crate::data_types::{NumberLike, UnsignedLike};
use crate::errors::PcoResult;
use crate::standalone::constants::{MAGIC_HEADER, MAGIC_TERMINATION_BYTE};
use crate::{bit_reader, wrapped, ChunkConfig, ChunkMetadata};

pub struct FileCompressor(wrapped::FileCompressor);

impl FileCompressor {
  pub fn new() -> Self {
    Self(wrapped::FileCompressor::new())
  }

  pub fn header_size_hint(&self) -> usize {
    MAGIC_HEADER.len() + self.0.header_size_hint()
  }

  pub fn write_header<'a>(&self, dst: &'a mut [u8]) -> PcoResult<&'a mut [u8]> {
    let mut extension = bit_reader::make_extension_for(dst, 0);
    let mut writer = BitWriter::new(dst, &mut extension);
    writer.write_aligned_bytes(&MAGIC_HEADER)?;
    let consumed = writer.bytes_consumed()?;
    self.0.write_header(&mut dst[consumed..])
  }

  pub fn chunk_compressor<T: NumberLike>(
    &self,
    nums: &[T],
    config: &ChunkConfig,
  ) -> PcoResult<ChunkCompressor<T::Unsigned>> {
    let mut config = config.clone();
    config.paging_spec = PagingSpec::ExactPageSizes(vec![nums.len()]);

    Ok(ChunkCompressor {
      inner: self.0.chunk_compressor(nums, &config)?,
      dtype_byte: T::DTYPE_BYTE,
    })
  }

  pub fn footer_size_hint(&self) -> usize {
    1
  }

  pub fn write_footer<'a>(&self, dst: &'a mut [u8]) -> PcoResult<&'a mut [u8]> {
    let mut extension = bit_reader::make_extension_for(dst, 0);
    let mut writer = BitWriter::new(dst, &mut extension);
    writer.write_aligned_bytes(&[MAGIC_TERMINATION_BYTE])?;
    let consumed = writer.bytes_consumed()?;
    Ok(&mut dst[consumed..])
  }
}

pub struct ChunkCompressor<U: UnsignedLike> {
  inner: wrapped::ChunkCompressor<U>,
  dtype_byte: u8,
}

impl<U: UnsignedLike> ChunkCompressor<U> {
  pub fn chunk_meta(&self) -> &ChunkMetadata<U> {
    self.inner.chunk_meta()
  }

  pub fn chunk_size_hint(&self) -> usize {
    1 + self.inner.chunk_meta_size_hint() + self.inner.page_size_hint(0)
  }

  pub fn write_chunk<'a>(&self, dst: &'a mut [u8]) -> PcoResult<&'a mut [u8]> {
    let mut extension = bit_reader::make_extension_for(dst, 1);
    let mut writer = BitWriter::new(dst, &mut extension);
    writer.write_aligned_bytes(&[self.dtype_byte])?;
    let consumed = writer.bytes_consumed()?;
    let dst = self.inner.write_chunk_meta(&mut dst[consumed..])?;
    self.inner.write_page(0, dst)
  }
}
