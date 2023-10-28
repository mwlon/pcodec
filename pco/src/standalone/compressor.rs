use std::io::Write;

use crate::bit_writer::BitWriter;
use crate::chunk_config::PagingSpec;
use crate::data_types::{NumberLike, UnsignedLike};
use crate::errors::PcoResult;
use crate::standalone::constants::{
  BITS_TO_ENCODE_N_ENTRIES, MAGIC_HEADER, MAGIC_TERMINATION_BYTE, STANDALONE_CHUNK_PREAMBLE_PADDING,
};
use crate::{bits, wrapped, ChunkConfig, ChunkMetadata};

#[derive(Clone, Debug, Default)]
pub struct FileCompressor(wrapped::FileCompressor);

impl FileCompressor {
  pub fn write_header<W: Write>(&self, dst: W) -> PcoResult<W> {
    let mut writer = BitWriter::new(dst, MAGIC_HEADER.len());
    writer.write_aligned_bytes(&MAGIC_HEADER)?;
    writer.flush()?;
    let dst = writer.finish();
    self.0.write_header(dst)
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

  pub fn write_footer<W: Write>(&self, dst: W) -> PcoResult<W> {
    let mut writer = BitWriter::new(dst, 1);
    writer.write_aligned_bytes(&[MAGIC_TERMINATION_BYTE])?;
    writer.flush()?;
    Ok(writer.finish())
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
    1 + bits::ceil_div(BITS_TO_ENCODE_N_ENTRIES as usize, 8)
      + self.inner.chunk_meta_size_hint()
      + self.inner.page_size_hint(0)
  }

  pub fn write_chunk<W: Write>(&self, dst: W) -> PcoResult<W> {
    let mut writer = BitWriter::new(dst, STANDALONE_CHUNK_PREAMBLE_PADDING);
    writer.write_aligned_bytes(&[self.dtype_byte])?;
    let n = self.inner.page_sizes()[0];
    writer.write_usize(n - 1, BITS_TO_ENCODE_N_ENTRIES);

    writer.flush()?;
    let dst = writer.finish();
    let dst = self.inner.write_chunk_meta(dst)?;
    self.inner.write_page(0, dst)
  }
}
