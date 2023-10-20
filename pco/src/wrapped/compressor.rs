use crate::wrapped::chunk_compressor::BaseCompressor;
use crate::chunk_spec::ChunkSpec;
use crate::data_types::NumberLike;
use crate::errors::PcoResult;
use crate::ChunkMetadata;
use crate::compressor_config::CompressorConfig;

/// Converts vectors of numbers into compressed bytes for use in a wrapping
/// columnar data format.
///
/// All compressor methods leave its state unchanged if they return an error.
/// You can configure behavior like compression level by instantiating with
/// [`.from_config()`][Compressor::from_config]
///
/// You can use the wrapped compressor at a data page level.
#[derive(Clone, Debug)]
pub struct Compressor<T: NumberLike>(BaseCompressor<T>);

impl<T: NumberLike> Default for Compressor<T> {
  fn default() -> Self {
    Self::from_config(CompressorConfig::default()).unwrap()
  }
}

impl<T: NumberLike> Compressor<T> {
  /// Creates a new compressor, given a [`CompressorConfig`].
  ///
  /// Internally, the compressor builds [`Flags`] as well as an internal
  /// configuration that doesn't show up in the output file.
  /// You can inspect the flags it chooses with [`.flags()`][Self::flags].
  ///
  /// Will return an error if the compressor config is invalid.
  pub fn from_config(config: CompressorConfig) -> PcoResult<Self> {
    Ok(Self(BaseCompressor::<T>::from_config(
      config, true,
    )?))
  }

  /// Returns a reference to the compressor's flags.
  pub fn flags(&self) -> &Flags {
    &self.0.flags
  }

  /// Writes out a header using the compressor's data type and flags.
  /// Will return an error if the compressor has already written the header.
  ///
  /// Each .pco file must start with such a header, which contains:
  /// * a 4-byte magic header for "pco!" in ascii,
  /// * a byte for the data type (e.g. `u32` has byte 1 and `f64` has byte
  /// 6), and
  /// * bytes for the flags used to compress.
  pub fn header(&mut self) -> PcoResult<()> {
    self.0.header()
  }

  /// Writes out and returns chunk metadata after training the compressor.
  /// Will return an error if the compressor has not yet written the header,
  /// in the middle of a chunk, or if the `spec` provided is
  /// incompatible with the count of `nums`.
  ///
  /// The `spec` indicates how the chunk's data pages will be broken up;
  /// see [`ChunkSpec`] for more detail.
  ///
  /// After this method, the compressor retains some precomputed information
  /// that only gets freed after every data page in the chunk has been written.
  pub fn chunk_metadata(
    &mut self,
    nums: &[T],
    spec: &ChunkSpec,
  ) -> PcoResult<ChunkMetadata<T::Unsigned>> {
    self.0.chunk_metadata_internal(nums, spec)
  }

  /// Writes out a data page, using precomputed data passed in through
  /// [`.chunk_metadata`][Self::chunk_metadata].
  /// Will return an error if the compressor is not at the start of a data
  /// page in the middle of a chunk.
  pub fn page(&mut self) -> PcoResult<()> {
    self.0.page_internal()
  }

  /// Returns all bytes produced by the compressor so far that have not yet
  /// been read.
  pub fn drain_bytes(&mut self) -> Vec<u8> {
    self.0.writer.drain_bytes()
  }

  /// Returns the number of bytes produced by the compressor so far that have
  /// not yet been read.
  pub fn byte_size(&mut self) -> usize {
    self.0.writer.byte_size()
  }
}
