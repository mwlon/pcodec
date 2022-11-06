use std::io::Write;
use crate::base_decompressor::{BaseDecompressor, Step};
use crate::{ChunkMetadata, DecompressorConfig, Flags};
use crate::data_types::NumberLike;
use crate::errors::QCompressResult;

#[derive(Clone, Debug, Default)]
pub struct Decompressor<T: NumberLike>(BaseDecompressor<T>);

impl<T: NumberLike> Decompressor<T> {
  /// Creates a new decompressor, given a [`DecompressorConfig`].
  pub fn from_config(config: DecompressorConfig) -> Self {
    Self(BaseDecompressor::<T>::from_config(config))
  }

  /// Reads the header, returning its [`Flags`] and updating this
  /// `Decompressor`'s state.
  /// Will return an error if this decompressor has already parsed a header,
  /// is not byte-aligned,
  /// runs out of data,
  /// finds flags from a newer, incompatible version of q_compress,
  /// or finds any corruptions.
  pub fn header(&mut self) -> QCompressResult<Flags> {
    self.0.header(true)
  }

  /// TODO
  pub fn chunk_metadata(&mut self) -> QCompressResult<Option<ChunkMetadata<T>>> {
    self.0.state.check_step_among(&[Step::StartOfChunk, Step::StartOfDataPage], "read chunk metadata")?;
    self.0.state.body_decompressor = None;

    self.0.chunk_metadata_internal()
  }
  // TODO
  pub fn data_page(
    &mut self,
    n: usize,
    compressed_page_size: usize,
  ) -> QCompressResult<Vec<T>> {
    self.0.data_page_internal(n, compressed_page_size)
  }

  /// Frees memory used for storing compressed bytes the decompressor has
  /// already decoded.
  /// Note that calling this too frequently can cause performance issues.
  pub fn free_compressed_memory(&mut self) {
    self.0.free_compressed_memory()
  }

  /// Returns the current bit position into the compressed data the
  /// decompressor is pointed at.
  /// Note that when memory is freed, this will decrease.
  pub fn bit_idx(&self) -> usize {
    self.0.bit_idx()
  }
}

impl<T: NumberLike> Write for Decompressor<T> {
  fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
    self.0.write(buf)
  }

  fn flush(&mut self) -> std::io::Result<()> {
    self.0.flush()
  }
}
