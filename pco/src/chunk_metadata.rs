use crate::bin::Bin;
use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;

use crate::constants::*;
use crate::data_types::{FloatLike, NumberLike, UnsignedLike};
use crate::delta_encoding::DeltaMoments;
use crate::errors::{PcoError, PcoResult};
use crate::float_mult_utils::FloatMultConfig;
use crate::lookback::LookbackMetadata;
use crate::modes::{gcd, Mode};
use crate::{bin, Flags};

/// Part of [`ChunkMetadata`][crate::ChunkMetadata] that describes a stream
/// interleaved into the compressed data.
///
/// For instance, with
/// [classic mode][crate::Mode::Classic], there is a single stream
/// corresponding to the actual numbers' (or deltas') bins and offsets
/// relative to those bins.
///
/// This is mainly useful for inspecting how compression was done.
#[derive(Clone, Debug, PartialEq)]
pub struct ChunkStreamMetadata<U: UnsignedLike> {
  /// The log2 of the number of the number of states in this chunk's tANS
  /// table.
  ///
  /// See <https://en.wikipedia.org/wiki/Asymmetric_numeral_systems>.
  pub ans_size_log: Bitlen,
  /// How the numbers or deltas are encoded, depending on their numerical
  /// range.
  pub bins: Vec<Bin<U>>,
  pub lookbacks: Vec<LookbackMetadata>,
}

impl<U: UnsignedLike> ChunkStreamMetadata<U> {
  fn parse_from(reader: &mut BitReader, mode: Mode<U>) -> PcoResult<Self> {
    let ans_size_log = reader.read_bitlen(BITS_TO_ENCODE_ANS_SIZE_LOG)?;
    let n_bins = reader.read_usize(BITS_TO_ENCODE_N_BINS)?;
    let n_lookbacks = reader.read_usize(BITS_TO_ENCODE_N_LOOKBACKS)?;
    let n_entries = n_bins + n_lookbacks;
    if 1 << ans_size_log < n_entries {
      return Err(PcoError::corruption(format!(
        "ANS size log ({}) is too small for number of bins and lookbacks ({})",
        ans_size_log, n_entries,
      )));
    }
    if n_bins == 0 && n_lookbacks > 0 {
      return Err(PcoError::corruption(format!(
        "No bins but {} lookbacks",
        n_lookbacks,
      )));
    }
    if n_entries == 1 && ans_size_log > 0 {
      return Err(PcoError::corruption(format!(
        "Only 1 bin but ANS size log is {} (should be 0)",
        ans_size_log,
      )));
    }

    let mut bins = Vec::with_capacity(n_bins);
    for _ in 0..n_bins {
      bins.push(Bin::<U>::parse_from(
        reader,
        mode,
        ans_size_log,
      )?);
    }

    let mut lookbacks = Vec::with_capacity(n_lookbacks);
    for _ in 0..n_lookbacks {
      lookbacks.push(LookbackMetadata::parse_from(
        reader,
        ans_size_log,
      )?);
    }

    Ok(Self {
      bins,
      ans_size_log,
      lookbacks,
    })
  }

  fn write_to(&self, mode: Mode<U>, writer: &mut BitWriter) {
    writer.write_bitlen(
      self.ans_size_log,
      BITS_TO_ENCODE_ANS_SIZE_LOG,
    );

    writer.write_usize(self.bins.len(), BITS_TO_ENCODE_N_BINS);
    writer.write_usize(
      self.lookbacks.len(),
      BITS_TO_ENCODE_N_LOOKBACKS,
    );

    for bin in &self.bins {
      bin.write_to(mode, self.ans_size_log, writer);
    }

    for lookback in &self.lookbacks {
      lookback.write_to(self.ans_size_log, writer);
    }
  }
}

#[derive(Clone, Debug)]
pub struct PageStreamMetadata<U: UnsignedLike> {
  pub delta_moments: DeltaMoments<U>,
  pub ans_final_state: usize,
}

impl<U: UnsignedLike> PageStreamMetadata<U> {
  pub fn write_to(&self, ans_size_log: Bitlen, writer: &mut BitWriter) {
    self.delta_moments.write_to(writer);

    // write the final ANS state, moving it down the range [0, table_size)
    writer.write_usize(
      self.ans_final_state - (1 << ans_size_log),
      ans_size_log,
    );
  }

  pub fn parse_from(
    reader: &mut BitReader,
    delta_order: usize,
    ans_size_log: Bitlen,
  ) -> PcoResult<Self> {
    let delta_moments = DeltaMoments::parse_from(reader, delta_order)?;
    let ans_final_state = (1 << ans_size_log) + reader.read_usize(ans_size_log)?;
    Ok(Self {
      delta_moments,
      ans_final_state,
    })
  }
}

/// The metadata of a pco chunk.
///
/// One can also create a rough histogram (or a histogram of deltas, if
/// delta encoding was used) by aggregating chunk metadata.
///
/// Each .pco file may contain multiple metadata sections, so to count the
/// entries, one must sum the count `n` for each chunk metadata. This can
/// be done easily - see the fast_seeking.rs example. For wrapped data,
/// `n` and `compressed_body_size` are not stored.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct ChunkMetadata<U: UnsignedLike> {
  /// The count of numbers in the chunk.
  /// Not available in wrapped mode.
  pub n: usize,
  /// The compressed byte length of the body that immediately follow this chunk
  /// metadata section.
  /// Not available in wrapped mode.
  pub compressed_body_size: usize,
  /// The formula `pco` used to compress each number at a low level.
  pub mode: Mode<U>,
  /// How many times delta encoding was applied during compression.
  /// This is stored as 3 bits to express 0-7.
  /// See [`CompressorConfig`][crate::CompressorConfig] for more details.
  pub delta_encoding_order: usize,
  /// The interleaved streams needed by `pco` to compress/decompress the inputs
  /// to the formula used by `mode`.
  pub streams: Vec<ChunkStreamMetadata<U>>,
}

// Data page metadata is slightly semantically different from chunk metadata,
// so it gets its own type.
// Importantly, `n` and `compressed_body_size` might come from either the
// chunk metadata parsing step (standalone mode) OR from the wrapping format
// (wrapped mode).
#[derive(Clone, Debug)]
pub struct PageMetadata<U: UnsignedLike> {
  pub streams: Vec<PageStreamMetadata<U>>,
}

impl<U: UnsignedLike> PageMetadata<U> {
  pub fn write_to<I: Iterator<Item = Bitlen>>(&self, ans_size_logs: I, writer: &mut BitWriter) {
    for (stream_idx, ans_size_log) in ans_size_logs.enumerate() {
      self.streams[stream_idx].write_to(ans_size_log, writer);
    }
    writer.finish_byte();
  }

  pub fn parse_from(reader: &mut BitReader, chunk_meta: &ChunkMetadata<U>) -> PcoResult<Self> {
    let mut streams = Vec::with_capacity(chunk_meta.streams.len());
    for (stream_idx, stream_meta) in chunk_meta.streams.iter().enumerate() {
      streams.push(PageStreamMetadata::parse_from(
        reader,
        chunk_meta.stream_delta_order(stream_idx),
        stream_meta.ans_size_log,
      )?);
    }
    reader.drain_empty_byte("non-zero bits at end of data page metadata")?;

    Ok(Self { streams })
  }
}

impl<U: UnsignedLike> ChunkMetadata<U> {
  pub(crate) fn new(
    n: usize,
    mode: Mode<U>,
    delta_encoding_order: usize,
    streams: Vec<ChunkStreamMetadata<U>>,
  ) -> Self {
    ChunkMetadata {
      n,
      compressed_body_size: 0,
      mode,
      delta_encoding_order,
      streams,
    }
  }

  pub(crate) fn parse_from(reader: &mut BitReader, flags: &Flags) -> PcoResult<Self> {
    let (n, compressed_body_size) = if flags.use_wrapped_mode {
      (0, 0)
    } else {
      (
        reader.read_usize(BITS_TO_ENCODE_N_ENTRIES)?,
        reader.read_usize(BITS_TO_ENCODE_COMPRESSED_BODY_SIZE)?,
      )
    };

    let mode = match reader.read_usize(BITS_TO_ENCODE_MODE)? {
      0 => Ok(Mode::Classic),
      1 => Ok(Mode::Gcd),
      2 => {
        let base = U::Float::from_unsigned(reader.read_uint::<U>(U::BITS)?);
        Ok(Mode::FloatMult(FloatMultConfig {
          base,
          inv_base: base.inv(),
        }))
      }
      value => Err(PcoError::compatibility(format!(
        "unknown mode value {}",
        value
      ))),
    }?;

    let delta_encoding_order = reader.read_usize(BITS_TO_ENCODE_DELTA_ENCODING_ORDER)?;

    let n_streams = mode.n_streams();

    let mut streams = Vec::with_capacity(n_streams);
    for _ in 0..n_streams {
      streams.push(ChunkStreamMetadata::parse_from(
        reader, mode,
      )?)
    }

    reader.drain_empty_byte("nonzero bits in end of final byte of chunk metadata")?;

    Ok(Self {
      n,
      compressed_body_size,
      mode,
      delta_encoding_order,
      streams,
    })
  }

  pub(crate) fn write_to(&self, flags: &Flags, writer: &mut BitWriter) {
    if !flags.use_wrapped_mode {
      writer.write_usize(self.n, BITS_TO_ENCODE_N_ENTRIES);
      writer.write_usize(
        self.compressed_body_size,
        BITS_TO_ENCODE_COMPRESSED_BODY_SIZE,
      );
    }

    let mode_value = match self.mode {
      Mode::Classic => 0,
      Mode::Gcd => 1,
      Mode::FloatMult { .. } => 2,
    };
    writer.write_usize(mode_value, BITS_TO_ENCODE_MODE);
    if let Mode::FloatMult(config) = self.mode {
      writer.write_diff(config.base.to_unsigned(), U::BITS);
    }

    writer.write_usize(
      self.delta_encoding_order,
      BITS_TO_ENCODE_DELTA_ENCODING_ORDER,
    );

    for stream in &self.streams {
      stream.write_to(self.mode, writer);
    }

    writer.finish_byte();
  }

  pub(crate) fn update_write_compressed_body_size(&self, writer: &mut BitWriter, bit_idx: usize) {
    writer.overwrite_usize(
      bit_idx + BITS_TO_ENCODE_N_ENTRIES as usize + 8,
      self.compressed_body_size,
      BITS_TO_ENCODE_COMPRESSED_BODY_SIZE,
    );
  }

  pub(crate) fn nontrivial_gcd_and_n_streams(&self) -> (bool, usize) {
    let primary_bins = &self.streams[0].bins;
    match self.mode {
      Mode::Classic | Mode::Gcd => {
        if bin::bins_are_trivial(primary_bins) {
          (false, 0)
        } else {
          let needs_gcd = gcd::use_gcd_arithmetic(primary_bins);
          (needs_gcd, 1)
        }
      }
      Mode::FloatMult(_) => {
        let n_streams = if bin::bins_are_trivial(&self.streams[1].bins) {
          if bin::bins_are_trivial(primary_bins) {
            0
          } else {
            1
          }
        } else {
          2
        };
        (false, n_streams)
      }
    }
  }

  pub(crate) fn stream_delta_order(&self, stream_idx: usize) -> usize {
    self
      .mode
      .stream_delta_order(stream_idx, self.delta_encoding_order)
  }
}

#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub enum PagingSpec {
  #[default]
  SinglePage,
  ExactPageSizes(Vec<usize>),
}
