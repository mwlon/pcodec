use crate::bit_writer::BitWriter;
use crate::chunk_config::PagingSpec;
use crate::data_types::{NumberLike, UnsignedLike};
use crate::errors::PcoResult;
use crate::standalone::constants::{BITS_TO_ENCODE_COMPRESSED_BODY_SIZE, BITS_TO_ENCODE_N_ENTRIES, MAGIC_HEADER, MAGIC_TERMINATION_BYTE};
use crate::{bit_reader, wrapped, ChunkConfig, ChunkMetadata};
use crate::constants::MINIMAL_PADDING_BYTES;

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
    let dst_len = dst.len();
    let mut extension = bit_reader::make_extension_for(dst, MINIMAL_PADDING_BYTES);
    let mut writer = BitWriter::new(dst, &mut extension);
    writer.write_aligned_bytes(&[self.dtype_byte])?;
    writer.write_usize(self.inner.page_sizes()[0], BITS_TO_ENCODE_N_ENTRIES);
    let compressed_body_size_byte_idx = writer.aligned_dst_byte_idx()?;
    writer.write_usize(0, BITS_TO_ENCODE_COMPRESSED_BODY_SIZE); // to be filled in later

    let preamble_consumed = writer.bytes_consumed()?;
    let final_consumed = {
      let new_dst_len = self.inner.write_chunk_meta(&mut dst[preamble_consumed..])?.len();
      dst_len - new_dst_len
    };
    let compressed_body_size = final_consumed - preamble_consumed;

    // go back and write in the compressed body size
    {
      let dst = &mut dst[compressed_body_size_byte_idx..];
      let mut ext = bit_reader::make_extension_for(dst, MINIMAL_PADDING_BYTES);
      let mut writer = BitWriter::new(dst, &mut ext);
      writer.write_uint(compressed_body_size, BITS_TO_ENCODE_COMPRESSED_BODY_SIZE);
    }

    self.inner.write_page(0, &mut dst[final_consumed..])
  }
}
