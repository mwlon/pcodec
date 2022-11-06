use crate::{ChunkMetadata, CompressorConfig, Flags};
use crate::base_compressor::BaseCompressor;
use crate::chunk_metadata::ChunkSpec;
use crate::data_types::NumberLike;
use crate::errors::QCompressResult;

#[derive(Clone, Debug)]
pub struct Compressor<T: NumberLike>(BaseCompressor<T>);

impl<T: NumberLike> Default for Compressor<T> {
  fn default() -> Self {
    Self::from_config(CompressorConfig::default())
  }
}

impl<T: NumberLike> Compressor<T> {
  /// Creates a new compressor, given a [`CompressorConfig`].
  /// Internally, the compressor builds [`Flags`] as well as an internal
  /// configuration that doesn't show up in the output file.
  /// You can inspect the flags it chooses with [`.flags()`][Self::flags].
  pub fn from_config(config: CompressorConfig) -> Self {
    Self(BaseCompressor::<T>::from_config(config, true))
  }

  /// Returns a reference to the compressor's flags.
  pub fn flags(&self) -> &Flags {
    &self.0.flags
  }

  /// Writes out a header using the compressor's data type and flags.
  /// Will return an error if the compressor has already written the header.
  ///
  /// Each .qco file must start with such a header, which contains:
  /// * a 4-byte magic header for "qco!" in ascii,
  /// * a byte for the data type (e.g. `i64` has byte 1 and `f64` has byte
  /// 5), and
  /// * bytes for the flags used to compress.
  pub fn header(&mut self) -> QCompressResult<()> {
    self.0.header()
  }

  /// TODO: documentation
  pub fn chunk_metadata(
    &mut self,
    nums: &[T],
    spec: &ChunkSpec,
  ) -> QCompressResult<ChunkMetadata<T>> {
    self.0.chunk_metadata_internal(nums, spec)
  }

  /// TODO documentation
  pub fn data_page(&mut self) -> QCompressResult<bool> {
    self.0.data_page_internal()
  }

  /// Writes out a single footer byte indicating that the .qco file has ended.
  /// Will return an error if the compressor has not yet written the header
  /// or already written the footer.
  pub fn footer(&mut self) -> QCompressResult<()> {
    self.0.footer()
  }

  /// Returns all bytes produced by the compressor so far that have not yet
  /// been read.
  ///
  /// In the future we may implement a method to write to a `std::io::Write` or
  /// implement `Compressor` as `std::io::Read`, TBD.
  pub fn drain_bytes(&mut self) -> Vec<u8> {
    self.0.writer.drain_bytes()
  }

  /// Returns the number of bytes produced by the compressor so far that have
  /// not yet been read.
  pub fn byte_size(&mut self) -> usize {
    self.0.writer.byte_size()
  }
}
