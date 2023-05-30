use crate::bin::Bin;
use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::constants::*;
use crate::data_types::UnsignedLike;
use crate::errors::{QCompressError, QCompressResult};
use crate::modes::gcd;
use crate::{bits, Flags};

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
  /// *How* the chunk body was compressed.
  pub bins: Vec<Bin<U>>,
}

fn parse_bins<U: UnsignedLike>(
  reader: &mut BitReader,
  flags: &Flags,
  n: usize,
) -> QCompressResult<Vec<Bin<U>>> {
  let n_bins = reader.read_usize(BITS_TO_ENCODE_N_BINS)?;
  let mut bins = Vec::with_capacity(n_bins);
  let bits_to_encode_count = flags.bits_to_encode_count(n);
  let maybe_common_gcd = if reader.read_one()? {
    Some(reader.read_uint::<U>(U::BITS)?)
  } else {
    None
  };
  let offset_bits_bits = bits::bits_to_encode_offset_bits::<U>();
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
    let gcd = if offset_bits == 0 {
      U::ONE
    } else if let Some(common_gcd) = maybe_common_gcd {
      common_gcd
    } else {
      reader.read_uint(U::BITS)?
    };
    bins.push(Bin {
      count,
      code,
      code_len,
      lower,
      offset_bits,
      run_len_jumpstart,
      gcd,
      float_mult_base: U::Float::default(),
      adj_bits: U::BITS,
    });
  }
  Ok(bins)
}

fn write_bins<U: UnsignedLike>(bins: &[Bin<U>], writer: &mut BitWriter, flags: &Flags, n: usize) {
  writer.write_usize(bins.len(), BITS_TO_ENCODE_N_BINS);
  let bits_to_encode_count = flags.bits_to_encode_count(n);
  let maybe_common_gcd = gcd::common_gcd_for_chunk_meta(bins);
  writer.write_one(maybe_common_gcd.is_some());
  if let Some(common_gcd) = maybe_common_gcd {
    writer.write_diff(common_gcd, U::BITS);
  }
  let offset_bits_bits = bits::bits_to_encode_offset_bits::<U>();
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
    if bin.offset_bits > 0 && maybe_common_gcd.is_none() {
      writer.write_diff(bin.gcd, U::BITS);
    }
  }
}

impl<U: UnsignedLike> ChunkMetadata<U> {
  pub(crate) fn new(n: usize, bins: Vec<Bin<U>>) -> Self {
    ChunkMetadata {
      n,
      compressed_body_size: 0,
      bins,
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

    let bins = parse_bins::<U>(reader, flags, n)?;

    reader.drain_empty_byte("nonzero bits in end of final byte of chunk metadata")?;

    Ok(Self {
      n,
      compressed_body_size,
      bins,
    })
  }

  pub(crate) fn write_to(&self, writer: &mut BitWriter, flags: &Flags) {
    if !flags.use_wrapped_mode {
      writer.write_usize(self.n, BITS_TO_ENCODE_N_ENTRIES);
      writer.write_usize(
        self.compressed_body_size,
        BITS_TO_ENCODE_COMPRESSED_BODY_SIZE,
      );
    }
    write_bins(&self.bins, writer, flags, self.n);
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
