use std::fmt::Debug;

use better_io::BetterBufRead;

use crate::bit_reader;
use crate::bit_reader::BitReaderBuilder;
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
  /// Reads a short header and returns a `FileDecompressor` and the remaining
  /// input.
  ///
  /// Will return an error if any version incompatibilities or
  /// insufficient data are found.
  pub fn new<R: BetterBufRead>(mut src: R) -> PcoResult<(Self, R)> {
    bit_reader::ensure_buf_read_capacity(&mut src, HEADER_PADDING);
    let mut reader_builder = BitReaderBuilder::new(src, HEADER_PADDING, 0);
    let format_version = reader_builder.with_reader(FormatVersion::parse_from)?;
    Ok((
      Self { format_version },
      reader_builder.into_inner(),
    ))
  }

  pub fn format_version(&self) -> u8 {
    self.format_version.0
  }

  /// Reads a chunk's metadata and returns a `ChunkDecompressor` and the
  /// remaining input.
  ///
  /// Will return an error if version incompatibilities, corruptions, or
  /// insufficient data are found.
  pub fn chunk_decompressor<T: NumberLike, R: BetterBufRead>(
    &self,
    mut src: R,
  ) -> PcoResult<(ChunkDecompressor<T>, R)> {
    bit_reader::ensure_buf_read_capacity(&mut src, CHUNK_META_PADDING);
    let mut reader_builder = BitReaderBuilder::new(src, CHUNK_META_PADDING, 0);
    let chunk_meta =
      ChunkMeta::<T::Unsigned>::parse_from(&mut reader_builder, &self.format_version)?;
    let cd = ChunkDecompressor::from(chunk_meta);
    Ok((cd, reader_builder.into_inner()))
  }
}
