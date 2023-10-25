use crate::bit_writer::BitWriter;
use crate::data_types::NumberLike;
use crate::errors::PcoResult;
use crate::format_version::FormatVersion;
use crate::wrapped::chunk_compressor;
use crate::wrapped::chunk_compressor::ChunkCompressor;
use crate::{bit_reader, ChunkConfig};

#[derive(Clone, Debug, Default)]
pub struct FileCompressor {
  format_version: FormatVersion,
}

impl FileCompressor {
  pub fn header_size_hint(&self) -> usize {
    1
  }

  pub fn write_header(&self, dst: &mut [u8]) -> PcoResult<usize> {
    let mut extension = bit_reader::make_extension_for(dst, 0);
    let mut writer = BitWriter::new(dst, &mut extension);
    self.format_version.write_to(&mut writer)?;
    writer.bytes_consumed()
  }

  pub fn chunk_compressor<T: NumberLike>(
    &self,
    nums: &[T],
    config: &ChunkConfig,
  ) -> PcoResult<ChunkCompressor<T::Unsigned>> {
    chunk_compressor::new(nums, config)
  }
}
