use std::io::Write;

use crate::bit_writer::BitWriter;
use crate::chunk_config::PagingSpec;
use crate::data_types::{Latent, NumberLike};
use crate::errors::PcoResult;
use crate::standalone::constants::{
  BITS_TO_ENCODE_N_ENTRIES, BITS_TO_ENCODE_STANDALONE_VERSION, CURRENT_STANDALONE_VERSION,
  MAGIC_HEADER, MAGIC_TERMINATION_BYTE, STANDALONE_CHUNK_PREAMBLE_PADDING,
  STANDALONE_HEADER_PADDING,
};
use crate::{bits, wrapped, ChunkConfig, ChunkMeta};

fn write_varint<W: Write>(n: u64, writer: &mut BitWriter<W>) {
  let power = if n == 0 { 1 } else { n.ilog2() + 1 };
  writer.write_uint(power - 1, 6);
  writer.write_uint(bits::lowest_bits(n, power), power);
}

/// Top-level entry point for compressing standalone .pco files.
///
/// Example of the lowest level API for writing a .pco file:
/// ```
/// use pco::ChunkConfig;
/// use pco::standalone::FileCompressor;
/// # use pco::errors::PcoResult;
///
/// # fn main() -> PcoResult<()> {
/// let mut compressed = Vec::new();
/// let file_compressor = FileCompressor::default();
/// file_compressor.write_header(&mut compressed)?;
/// for chunk in [vec![1, 2, 3], vec![4, 5]] {
///   let mut chunk_compressor = file_compressor.chunk_compressor::<i64>(
///     &chunk,
///     &ChunkConfig::default(),
///   )?;
///   chunk_compressor.write_chunk(&mut compressed)?;
/// }
/// file_compressor.write_footer(&mut compressed)?;
/// // now `compressed` is a complete .pco file with 2 chunks
/// # Ok(())
/// # }
/// ```
///
/// A .pco file should contain a header, followed by any number of chunks,
/// followed by a footer.
#[derive(Clone, Debug, Default)]
pub struct FileCompressor {
  inner: wrapped::FileCompressor,
  n_hint: usize,
}

impl FileCompressor {
  pub fn with_n_hint(mut self, n: usize) -> Self {
    self.n_hint = n;
    self
  }

  /// Writes a short header to the destination.
  ///
  /// Will return an error if the provided `Write` errors.
  pub fn write_header<W: Write>(&self, dst: W) -> PcoResult<W> {
    let mut writer = BitWriter::new(dst, STANDALONE_HEADER_PADDING);
    writer.write_aligned_bytes(&MAGIC_HEADER)?;
    writer.write_usize(
      CURRENT_STANDALONE_VERSION,
      BITS_TO_ENCODE_STANDALONE_VERSION,
    );
    write_varint(self.n_hint as u64, &mut writer);
    writer.finish_byte();
    writer.flush()?;
    let dst = writer.into_inner();
    self.inner.write_header(dst)
  }

  /// Creates a `ChunkCompressor` that can be used to write entire chunks
  /// at a time.
  ///
  /// Will return an error if any arguments provided are invalid.
  ///
  /// Although this doesn't write anything yet, it does the bulk of
  /// compute necessary for the compression.
  pub fn chunk_compressor<T: NumberLike>(
    &self,
    nums: &[T],
    config: &ChunkConfig,
  ) -> PcoResult<ChunkCompressor<T::L>> {
    let mut config = config.clone();
    config.paging_spec = PagingSpec::ExactPageSizes(vec![nums.len()]);

    Ok(ChunkCompressor {
      inner: self.inner.chunk_compressor(nums, &config)?,
      dtype_byte: T::DTYPE_BYTE,
    })
  }

  /// Writes a short footer to the destination.
  ///
  /// Will return an error if the provided `Write` errors.
  pub fn write_footer<W: Write>(&self, dst: W) -> PcoResult<W> {
    let mut writer = BitWriter::new(dst, 1);
    writer.write_aligned_bytes(&[MAGIC_TERMINATION_BYTE])?;
    writer.flush()?;
    Ok(writer.into_inner())
  }
}

/// Holds metadata about a chunk and supports compression.
#[derive(Clone, Debug)]
pub struct ChunkCompressor<U: Latent> {
  inner: wrapped::ChunkCompressor<U>,
  dtype_byte: u8,
}

impl<U: Latent> ChunkCompressor<U> {
  /// Returns pre-computed information about the chunk.
  pub fn meta(&self) -> &ChunkMeta<U> {
    self.inner.meta()
  }

  /// Returns an estimate of the overall size of the chunk.
  ///
  /// This can be useful when building the file as a `Vec<u8>` in memory;
  /// you can `.reserve(chunk_compressor.chunk_size_hint())` ahead of time.
  pub fn chunk_size_hint(&self) -> usize {
    1 + bits::ceil_div(BITS_TO_ENCODE_N_ENTRIES as usize, 8)
      + self.inner.chunk_meta_size_hint()
      + self.inner.page_size_hint(0)
  }

  /// Writes an entire chunk to the destination.
  ///
  /// Will return an error if the provided `Write` errors.
  pub fn write_chunk<W: Write>(&self, dst: W) -> PcoResult<W> {
    let mut writer = BitWriter::new(dst, STANDALONE_CHUNK_PREAMBLE_PADDING);
    writer.write_aligned_bytes(&[self.dtype_byte])?;
    let n = self.inner.n_per_page()[0];
    writer.write_usize(n - 1, BITS_TO_ENCODE_N_ENTRIES);

    writer.flush()?;
    let dst = writer.into_inner();
    let dst = self.inner.write_chunk_meta(dst)?;
    self.inner.write_page(0, dst)
  }
}
