use std::io::Write;

use crate::bit_writer::BitWriter;
use crate::constants::HEADER_PADDING;
use crate::data_types::NumberLike;
use crate::errors::PcoResult;
use crate::format_version::FormatVersion;
use crate::wrapped::chunk_compressor;
use crate::wrapped::chunk_compressor::ChunkCompressor;
use crate::ChunkConfig;

#[derive(Clone, Debug, Default)]
pub struct FileCompressor {
  format_version: FormatVersion,
}

impl FileCompressor {
  // pub fn header_size_hint(&self) -> usize {
  //   1
  // }
  //
  pub fn write_header<W: Write>(&self, dst: W) -> PcoResult<W> {
    let mut writer = BitWriter::new(dst, HEADER_PADDING);
    self.format_version.write_to(&mut writer)?;
    writer.flush()?;
    Ok(writer.finish())
  }

  pub fn chunk_compressor<T: NumberLike>(
    &self,
    nums: &[T],
    config: &ChunkConfig,
  ) -> PcoResult<ChunkCompressor<T::Unsigned>> {
    chunk_compressor::new(nums, config)
  }
}
