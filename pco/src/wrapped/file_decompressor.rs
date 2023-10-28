use std::fmt::Debug;

use crate::bit_reader;
use crate::bit_reader::BitReader;
use crate::chunk_meta::ChunkMeta;
use crate::constants::{CHUNK_META_PADDING, HEADER_PADDING};
use crate::data_types::NumberLike;
use crate::errors::PcoResult;
use crate::format_version::FormatVersion;
use crate::wrapped::chunk_decompressor::ChunkDecompressor;

/// Top-level entry point for decompressing wrapped pco files.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct FileDecompressor {
  format_version: FormatVersion,
}

impl FileDecompressor {
  /// Reads a short header and returns a `FileDecompressor` and the number of
  /// bytes read.
  ///
  /// Will return an error if any version incompatibilities or
  /// insufficient data are found.
  pub fn new(src: &[u8]) -> PcoResult<(Self, usize)> {
    let extension = bit_reader::make_extension_for(src, HEADER_PADDING);
    let mut reader = BitReader::new(src, &extension);
    let format_version = FormatVersion::parse_from(&mut reader)?;
    Ok((
      Self { format_version },
      reader.bytes_consumed()?,
    ))
  }

  pub fn format_version(&self) -> u8 {
    self.format_version.0
  }

  /// Reads a chunk's metadata and returns a `ChunkDecompressor` and the
  /// number of bytes read.
  ///
  /// Will return an error if corruptions or insufficient data are found.
  pub fn chunk_decompressor<T: NumberLike>(
    &self,
    src: &[u8],
  ) -> PcoResult<(ChunkDecompressor<T>, usize)> {
    let extension = bit_reader::make_extension_for(src, CHUNK_META_PADDING);
    let mut reader = BitReader::new(src, &extension);
    let chunk_meta = ChunkMeta::<T::Unsigned>::parse_from(&mut reader, &self.format_version)?;
    let cd = ChunkDecompressor::from(chunk_meta);
    Ok((cd, reader.bytes_consumed()?))
  }
}
