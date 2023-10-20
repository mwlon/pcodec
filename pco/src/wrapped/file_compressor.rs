use std::io;
use std::io::Write;
use crate::bit_writer::BitWriter;
use crate::data_types::NumberLike;
use crate::errors::PcoResult;
use crate::format_version::FormatVersion;
use crate::wrapped::chunk_compressor::ChunkCompressor;

pub struct FileCompressor {

}

impl FileCompressor {
  pub fn new(dst: &mut [u8]) -> PcoResult<Self> {
    let writer = BitWriter::new(dst);
    writer.
  }

  pub fn chunk_compressor<T: NumberLike, W: Write>(&self, dst: W) -> PcoResult<ChunkCompressor<T>> {

  }
}
