use crate::chunk_spec::ChunkSpec;
use crate::standalone::constants::MAGIC_TERMINATION_BYTE;
use crate::data_types::NumberLike;
use crate::errors::PcoResult;
use crate::ChunkMetadata;
use crate::compressor_config::CompressorConfig;
use crate::wrapped::chunk_compressor::{ChunkCompressor, State};

/// Converts vectors of numbers into compressed bytes in .pco format.
///
/// Most compressor methods leave its state unchanged if they return an error.
/// You can configure behavior like compression level by instantiating with
/// [`.from_config()`][Compressor::from_config]
///
/// You can use the standalone compressor at a chunk level.
/// ```
/// use pco::standalone::Compressor;
///
/// let my_nums = vec![1, 2, 3];
///
/// let mut compressor = Compressor::<i32>::default();
/// compressor.header().expect("header");
/// compressor.chunk(&my_nums).expect("chunk");
/// compressor.footer().expect("footer");
/// let bytes = compressor.drain_bytes();
/// ```
/// Note that in practice we would need larger chunks than this to
/// achieve good compression, preferably containing 2k-10M numbers.
#[derive(Clone, Debug)]
pub struct Compressor<T: NumberLike>(ChunkCompressor<T>);

impl<T: NumberLike> Default for Compressor<T> {
  fn default() -> Self {
    Self::from_config(CompressorConfig::default()).unwrap()
  }
}

impl<T: NumberLike> Compressor<T> {
  /// Creates a new compressor, given a [`CompressorConfig`].
  ///
  /// Internally, the compressor builds [`FormatVersion`] as well as an internal
  /// configuration that doesn't show up in the output file.
  /// You can inspect the flags it chooses with [`.flags()`][Self::flags].
  ///
  /// Will return an error if the compressor config is invalid.
  pub fn from_config(config: CompressorConfig) -> PcoResult<Self> {
    Ok(Self(ChunkCompressor::<T>::from_config(
      config, false,
    )?))
  }

  /// Returns a reference to the compressor's flags.
  pub fn flags(&self) -> &FormatVersion {
    &self.0.format_version
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

  /// Writes out a chunk of data representing the provided numbers.
  /// Will return an error if the compressor has not yet written the header
  /// or already written the footer.
  ///
  /// Each chunk contains a [`ChunkMetadata`] section followed by the chunk body.
  /// The chunk body encodes the numbers passed in here.
  pub fn chunk(&mut self, nums: &[T]) -> PcoResult<ChunkMetadata<T::Unsigned>> {
    let pre_meta_bit_idx = self.0.writer.bit_size();
    let mut meta = self
      .0
      .chunk_metadata_internal(nums, &ChunkSpec::default())?;
    let post_meta_byte_idx = self.0.writer.byte_size();

    self.0.page_internal()?;

    meta.compressed_body_size = self.0.writer.byte_size() - post_meta_byte_idx;
    meta.update_write_compressed_body_size(&mut self.0.writer, pre_meta_bit_idx);
    Ok(meta)
  }

  /// Writes out a single footer byte indicating that the .pco file has ended.
  /// Will return an error if the compressor has not yet written the header
  /// or already written the footer.
  pub fn footer(&mut self) -> PcoResult<()> {
    if !matches!(self.0.state, State::StartOfChunk) {
      return Err(self.0.state.wrong_step_err("footer"));
    }

    self.0.writer.write_aligned_byte(MAGIC_TERMINATION_BYTE)?;
    self.0.state = State::Terminated;
    Ok(())
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
