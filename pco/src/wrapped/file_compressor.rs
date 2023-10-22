use std::io;
use std::io::Write;
use crate::{bit_writer, ChunkConfig};
use crate::bit_writer::BitWriter;
use crate::constants::CURRENT_FORMAT_VERSION;
use crate::data_types::NumberLike;
use crate::errors::PcoResult;
use crate::format_version::FormatVersion;
use crate::wrapped::chunk_compressor::ChunkCompressor;

pub struct FileCompressor {
  format_version: FormatVersion,
}

impl FileCompressor {
  pub fn new() -> Self {
    Self {
      format_version: FormatVersion(CURRENT_FORMAT_VERSION),
    }
  }

  pub fn header_size_hint(&self) -> usize {
    1
  }

  pub fn write_header(&self, dst: &mut [u8]) -> PcoResult<&mut [u8]> {
    let mut extension = bit_writer::make_extension_for(dst, 0);
    let mut writer = BitWriter::new(dst, &mut extension);
    self.format_version.write_to(&mut writer)?;
    writer.rest()
  }

  pub fn chunk_compressor<T: NumberLike>(&self, nums: &[T], config: &ChunkConfig) -> PcoResult<ChunkCompressor<T>> {
    ChunkCompressor::new(nums, config)
  }
}
