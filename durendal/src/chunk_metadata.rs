use crate::bin::Bin;
use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::constants::*;
use crate::data_types::{NumberLike, UnsignedLike, FloatLike};
use crate::errors::{QCompressError, QCompressResult};
use crate::modes::DynMode;
use crate::{bits, Flags};
use crate::bits::bits_to_encode_offset_bits;
use crate::delta_encoding::DeltaMoments;
use crate::float_mult_utils::FloatMultConfig;

/// The metadata of a Quantile-compressed chunk.
///
/// One can also create a rough histogram (or a histogram of deltas, if
/// delta encoding was used) by aggregating chunk metadata.
///
/// Each .qco file may contain multiple metadata sections, so to count the
/// entries, one must sum the count `n` for each chunk metadata. This can
/// be done easily - see the fast_seeking.rs example. For wrapped data,
/// `n` and `compressed_body_size` are not stored.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct ChunkMetadata<U: UnsignedLike> {
  /// The count of numbers in the chunk.
  /// Not available in wrapped mode.
  pub n: usize,
  /// The compressed byte length of the body that immediately follow this chunk metadata section.
  /// Not available in wrapped mode.
  pub compressed_body_size: usize,
  pub dyn_mode: DynMode<U>,
  /// *How* the chunk body was compressed.
  pub bins: Vec<Bin<U>>,
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
  pub dyn_mode: DynMode<U>,
  pub bins: &'a [Bin<U>],
  pub delta_moments: DeltaMoments<U>,
}

fn parse_bins<U: UnsignedLike>(
  reader: &mut BitReader,
  flags: &Flags,
  dyn_mode: DynMode<U>,
  n: usize,
) -> QCompressResult<Vec<Bin<U>>> {
  let n_bins = reader.read_usize(BITS_TO_ENCODE_N_BINS)?;
  let mut bins = Vec::with_capacity(n_bins);
  let bits_to_encode_count = flags.bits_to_encode_count(n);
  let offset_bits_bits = bits_to_encode_offset_bits::<U>();
  for _ in 0..n_bins {
    let count = reader.read_usize(bits_to_encode_count)?;
    let lower = reader.read_uint::<U>(U::BITS)?;

    let offset_bits = reader.read_bitlen(offset_bits_bits)?;
    if offset_bits > U::BITS {
      return Err(QCompressError::corruption(format!(
        "offset bits of {} exceeds data type of {} bits",
        offset_bits,
        U::BITS,
      )));
    }

    let code_len = reader.read_bitlen(BITS_TO_ENCODE_CODE_LEN)?;
    let code = reader.read_usize(code_len)?;
    let run_len_jumpstart = if reader.read_one()? {
      Some(reader.read_bitlen(BITS_TO_ENCODE_JUMPSTART)?)
    } else {
      None
    };
    let mut bin = Bin {
      count,
      code,
      code_len,
      lower,
      offset_bits,
      run_len_jumpstart,
      gcd: U::ONE,
    };
    match dyn_mode {
      DynMode::Classic => (),
      DynMode::Gcd => {
        bin.gcd = if offset_bits == 0 {
          U::ONE
        } else {
          reader.read_uint(U::BITS)?
        };
      }
      DynMode::FloatMult { .. } => (),
    }
    bins.push(bin);
  }
  Ok(bins)
}

fn write_bins<U: UnsignedLike>(
  bins: &[Bin<U>],
  flags: &Flags,
  mode: DynMode<U>,
  n: usize,
  writer: &mut BitWriter,
) {
  writer.write_usize(bins.len(), BITS_TO_ENCODE_N_BINS);
  let bits_to_encode_count = flags.bits_to_encode_count(n);
  let offset_bits_bits = bits_to_encode_offset_bits::<U>();
  for bin in bins {
    writer.write_usize(bin.count, bits_to_encode_count);
    writer.write_diff(bin.lower, U::BITS);
    writer.write_bitlen(bin.offset_bits, offset_bits_bits);
    writer.write_bitlen(bin.code_len, BITS_TO_ENCODE_CODE_LEN);
    writer.write_usize(bin.code, bin.code_len);

    match bin.run_len_jumpstart {
      None => {
        writer.write_one(false);
      }
      Some(jumpstart) => {
        writer.write_one(true);
        writer.write_bitlen(jumpstart, BITS_TO_ENCODE_JUMPSTART);
      }
    }

    match mode {
      DynMode::Classic => (),
      DynMode::Gcd => {
        if bin.offset_bits > 0 {
          writer.write_diff(bin.gcd, U::BITS);
        }
      }
      DynMode::FloatMult { .. } => (),
    }
  }
}

impl<U: UnsignedLike> ChunkMetadata<U> {
  pub(crate) fn new(n: usize, bins: Vec<Bin<U>>, dyn_mode: DynMode<U>) -> Self {
    ChunkMetadata {
      n,
      compressed_body_size: 0,
      bins,
      dyn_mode,
    }
  }

  pub(crate) fn parse_from(reader: &mut BitReader, flags: &Flags) -> QCompressResult<Self> {
    let (n, compressed_body_size) = if flags.use_wrapped_mode {
      (0, 0)
    } else {
      (
        reader.read_usize(BITS_TO_ENCODE_N_ENTRIES)?,
        reader.read_usize(BITS_TO_ENCODE_COMPRESSED_BODY_SIZE)?,
      )
    };

    let dyn_mode = match reader.read_usize(BITS_TO_ENCODE_MODE)? {
      0 => Ok(DynMode::Classic),
      1 => Ok(DynMode::Gcd),
      2 => {
        let adj_bits = reader.read_bitlen(bits_to_encode_offset_bits::<U>())?;
        let base = U::Float::from_unsigned(reader.read_uint::<U>(U::BITS)?);
        Ok(DynMode::float_mult(FloatMultConfig { adj_bits, base, inv_base: base.inv() }))
      }
      value => Err(QCompressError::compatibility(format!(
        "unknown mode value {}",
        value
      ))),
    }?;

    let bins = parse_bins::<U>(reader, flags, dyn_mode, n)?;

    reader.drain_empty_byte("nonzero bits in end of final byte of chunk metadata")?;

    Ok(Self {
      n,
      compressed_body_size,
      bins,
      dyn_mode,
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

    let mode_value = match self.dyn_mode {
      DynMode::Classic => 0,
      DynMode::Gcd => 1,
      DynMode::FloatMult { .. } => 2,
    };
    writer.write_usize(mode_value, BITS_TO_ENCODE_MODE);
    if let DynMode::FloatMult { adj_bits, base, .. } = self.dyn_mode {
      writer.write_bitlen(adj_bits, bits_to_encode_offset_bits::<U>());
      writer.write_diff(base.to_unsigned(), U::BITS);
    }

    write_bins(
      &self.bins,
      flags,
      self.dyn_mode,
      self.n,
      writer,
    );
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
