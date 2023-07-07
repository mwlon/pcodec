use std::io::Write;

use crate::base_decompressor::{BaseDecompressor, Step};
use crate::bit_words::PaddedBytes;
use crate::data_types::NumberLike;
use crate::errors::PcoResult;
use crate::{ChunkMetadata, DecompressorConfig, Flags};

/// Converts wrapped pcodec data into [`Flags`], [`ChunkMetadata`], and vectors
/// of numbers.
///
/// All decompressor methods leave its state unchanged if they return an
/// error.
///
/// You can use the wrapped decompressor at a data page level.
/// This allows the wrapping format to use its own data page indices to support
/// complex filtering and seeking.
#[derive(Clone, Debug, Default)]
pub struct Decompressor<T: NumberLike>(BaseDecompressor<T>);

impl<T: NumberLike> Decompressor<T> {
  /// Creates a new decompressor, given a [`DecompressorConfig`].
  pub fn from_config(config: DecompressorConfig) -> Self {
    Self(BaseDecompressor::<T>::from_config(config))
  }

  /// Reads the header, returning its [`Flags`] and updating this
  /// decompressor's state.
  /// Will return an error if the decompressor has already parsed a header,
  /// is not byte-aligned,
  /// runs out of data,
  /// finds flags from a newer, incompatible version of pco,
  /// or finds any corruptions.
  pub fn header(&mut self) -> PcoResult<Flags> {
    self.0.header(true)
  }

  /// Reads the chunk metadata, returning its metadata and updating the
  /// decompressor's state.
  /// Will return an error if the decompressor has not parsed the header,
  /// runs out of data,
  /// or finds any corruptions.
  ///
  /// This can be used regardless of whether the decompressor has finished
  /// reading all data pages from the preceding chunk.
  pub fn chunk_metadata(&mut self) -> PcoResult<ChunkMetadata<T::Unsigned>> {
    self.0.state.check_step_among(
      &[Step::StartOfChunk, Step::StartOfDataPage, Step::MidDataPage],
      "read chunk metadata",
    )?;

    self.0.with_reader(|reader, state, _| {
      let meta = ChunkMetadata::<T::Unsigned>::parse_from(reader, state.flags.as_ref().unwrap())?;

      state.chunk_meta = Some(meta.clone());
      state.body_decompressor = None;
      Ok(meta)
    })
  }

  /// Initializes the decompressor for the next data page, reading in the
  /// data page's metadata but not the compressed body.
  /// Will return an error if the decompressor is not in a
  /// chunk, runs out of data, or finds any corruptions.
  ///
  /// This can be used regardless of whether the decompressor has finished
  /// reading the previous data page.
  pub fn begin_data_page(&mut self, n: usize, compressed_page_size: usize) -> PcoResult<()> {
    self.0.state.check_step_among(
      &[Step::StartOfDataPage, Step::MidDataPage],
      "begin data page",
    )?;
    self.0.with_reader(|reader, state, _| {
      state.body_decompressor =
        Some(state.new_body_decompressor(reader, n, compressed_page_size)?);
      Ok(())
    })
  }

  /// Reads up to `limit` numbers from the current data page.
  /// Will return an error if the decompressor is not in a data page,
  /// it runs out of data, or any corruptions are found.
  pub fn next_batch(&mut self, dest: &mut [T]) -> PcoResult<()> {
    self
      .0
      .state
      .check_step(Step::MidDataPage, "read next batch")?;
    self.0.with_reader(|reader, state, _| {
      let bd = state.body_decompressor.as_mut().unwrap();
      let batch_res = bd.decompress(reader, true, dest)?;
      if batch_res.finished_body {
        state.body_decompressor = None;
      }
      Ok(())
    })
  }

  /// Reads an entire data page, returning its numbers.
  /// Will return an error if the decompressor is not in a chunk,
  /// it runs out of data, or any corruptions are found.
  ///
  /// This is similar to calling [`.begin_data_page`][Self::begin_data_page] and then
  /// [`.next_batch(usize::MAX)`][Self::next_batch].
  pub fn data_page(
    &mut self,
    n: usize,
    compressed_page_size: usize,
    dest: &mut [T],
  ) -> PcoResult<()> {
    self.0.state.check_step_among(
      &[Step::StartOfDataPage, Step::MidDataPage],
      "data page",
    )?;
    self.0.data_page_internal(n, compressed_page_size, dest)
  }

  /// Frees memory used for storing compressed bytes the decompressor has
  /// already decoded.
  /// Note that calling this too frequently can cause performance issues.
  pub fn free_compressed_memory(&mut self) {
    self.0.free_compressed_memory()
  }

  /// Clears any data written to the decompressor but not yet decompressed.
  /// As an example, if you want to want to read the first 5 numbers from each
  /// data page, you might write each compressed data page to the decompressor,
  /// then repeatedly call
  /// [`.begin_data_page`][Self::begin_data_page],
  /// [`.next_nums`][Self::next_batch], and
  /// this method.
  pub fn clear_compressed_bytes(&mut self) {
    self.0.words = PaddedBytes::default();
    self.0.state.bit_idx = 0;
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
