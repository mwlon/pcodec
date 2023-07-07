use crate::bin::Bin;
use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::bits::bits_to_encode_offset_bits;
use crate::constants::*;
use crate::data_types::{FloatLike, NumberLike, UnsignedLike};
use crate::delta_encoding::DeltaMoments;
use crate::errors::{PcoError, PcoResult};
use crate::float_mult_utils::FloatMultConfig;
use crate::modes::{Mode};
use crate::Flags;

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
  /// How the numbers or deltas are encoded, depending on their numerical
  /// range.
  pub bins: Vec<Bin<U>>,
  /// The log2 of the number of the number of states in this chunk's tANS
  /// table.
  ///
  /// See <https://en.wikipedia.org/wiki/Asymmetric_numeral_systems>.
  pub ans_size_log: Bitlen,
}

impl<U: UnsignedLike> ChunkStreamMetadata<U> {
  fn parse_from(reader: &mut BitReader, mode: Mode<U>) -> PcoResult<Self> {
    let ans_size_log = reader.read_bitlen(BITS_TO_ENCODE_ANS_SIZE_LOG)?;
    let bins = parse_bins::<U>(reader, mode, ans_size_log)?;

    Ok(Self { bins, ans_size_log })
  }

  fn write_to(&self, mode: Mode<U>, writer: &mut BitWriter) {
    writer.write_bitlen(
      self.ans_size_log,
      BITS_TO_ENCODE_ANS_SIZE_LOG,
    );

    write_bins(
      &self.bins,
      mode,
      self.ans_size_log,
      writer,
    );
  }
}

#[derive(Clone, Debug)]
pub struct DataPageStreamMetadata<'a, U: UnsignedLike> {
  pub bins: &'a [Bin<U>],
  pub delta_moments: DeltaMoments<U>,
  pub ans_size_log: Bitlen,
  pub ans_final_state: usize,
}

impl<'a, U: UnsignedLike> DataPageStreamMetadata<'a, U> {
  pub fn new(
    chunk_stream_meta: &'a ChunkStreamMetadata<U>,
    delta_moments: DeltaMoments<U>,
    ans_final_state: usize,
  ) -> Self {
    Self {
      bins: &chunk_stream_meta.bins,
      delta_moments,
      ans_size_log: chunk_stream_meta.ans_size_log,
      ans_final_state,
    }
  }
}

/// The metadata of a pcodec chunk.
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
pub struct DataPageMetadata<'a, U: UnsignedLike> {
  pub n: usize,
  pub compressed_body_size: usize,
  pub mode: Mode<U>,
  pub streams: Vec<DataPageStreamMetadata<'a, U>>,
}

fn parse_bins<U: UnsignedLike>(
  reader: &mut BitReader,
  mode: Mode<U>,
  ans_size_log: Bitlen,
) -> PcoResult<Vec<Bin<U>>> {
  let n_bins = reader.read_usize(BITS_TO_ENCODE_N_BINS)?;
  let mut bins = Vec::with_capacity(n_bins);
  let offset_bits_bits = bits_to_encode_offset_bits::<U>();
  if 1 << ans_size_log < n_bins {
    return Err(PcoError::corruption(format!(
      "ANS size log ({}) is too small for number of bins ({})",
      ans_size_log, n_bins,
    )));
  }
  if n_bins == 1 && ans_size_log > 0 {
    return Err(PcoError::corruption(format!(
      "Only 1 bin but ANS size log is {} (should be 0)",
      ans_size_log,
    )));
  }
  for _ in 0..n_bins {
    let weight = reader.read_usize(ans_size_log)? + 1;
    let lower = reader.read_uint::<U>(U::BITS)?;

    let offset_bits = reader.read_bitlen(offset_bits_bits)?;
    if offset_bits > U::BITS {
      return Err(PcoError::corruption(format!(
        "offset bits of {} exceeds data type of {} bits",
        offset_bits,
        U::BITS,
      )));
    }

    let mut bin = Bin {
      weight,
      lower,
      offset_bits,
      gcd: U::ONE,
    };
    match mode {
      Mode::Classic => (),
      Mode::Gcd => {
        bin.gcd = if offset_bits == 0 {
          U::ONE
        } else {
          reader.read_uint(U::BITS)?
        };
      }
      Mode::FloatMult { .. } => (),
    }
    bins.push(bin);
  }
  Ok(bins)
}

fn write_bins<U: UnsignedLike>(
  bins: &[Bin<U>],
  mode: Mode<U>,
  ans_size_log: Bitlen,
  writer: &mut BitWriter,
) {
  writer.write_usize(bins.len(), BITS_TO_ENCODE_N_BINS);
  let offset_bits_bits = bits_to_encode_offset_bits::<U>();
  for bin in bins {
    writer.write_usize(bin.weight - 1, ans_size_log);
    writer.write_diff(bin.lower, U::BITS);
    writer.write_bitlen(bin.offset_bits, offset_bits_bits);

    match mode {
      Mode::Classic => (),
      Mode::Gcd => {
        if bin.offset_bits > 0 {
          writer.write_diff(bin.gcd, U::BITS);
        }
      }
      Mode::FloatMult { .. } => (),
    }
  }
}

impl<U: UnsignedLike> ChunkMetadata<U> {
  pub(crate) fn new(n: usize, mode: Mode<U>, streams: Vec<ChunkStreamMetadata<U>>) -> Self {
    ChunkMetadata {
      n,
      compressed_body_size: 0,
      mode,
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
}

#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub enum DataPagingSpec {
  #[default]
  SinglePage,
  ExactPageSizes(Vec<usize>),
}
