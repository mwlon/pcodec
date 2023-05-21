use crate::bin::Bin;
use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::constants::*;
use crate::data_types::{NumberLike, UnsignedLike};
use crate::errors::{QCompressError, QCompressResult};
use crate::{bits, gcd_utils, Flags};

/// A wrapper for bins in the two cases cases: delta encoded or not.
///
/// This is the part of chunk metadata that describes *how* the data was
/// compressed - the Huffman codes used and what ranges they specify.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum BinMetadata<T: NumberLike> {
  /// `Simple` bin metadata corresponds to the case when delta encoding is
  /// off (`delta_encoding_order` of 0).
  ///
  /// It simply contains bins of the same data type being compressed.
  Simple { bins: Vec<Bin<T>> },
  /// `Delta` bin metadata corresponds to the case when delta encoding is
  /// on.
  ///
  /// It contains bins of the associated `SignedLike` type (what the
  /// deltas are expressed as). For instance, a chunk of delta-encoded `f64`s
  /// with `delta_encoding_order: 1`
  /// will have bins of type `i64`, where a delta of n indicates a change
  /// of n * machine epsilon from the last float.
  #[non_exhaustive]
  Delta { bins: Vec<Bin<T::Unsigned>> },
}

impl<T: NumberLike> BinMetadata<T> {
  pub(crate) fn use_gcd(&self) -> bool {
    match self {
      BinMetadata::Simple { bins } => gcd_utils::use_gcd_arithmetic(bins),
      BinMetadata::Delta { bins } => gcd_utils::use_gcd_arithmetic(bins),
    }
  }
}

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
pub struct ChunkMetadata<T: NumberLike> {
  /// The count of numbers in the chunk.
  /// Not available in wrapped mode.
  pub n: usize,
  /// The compressed byte length of the body that immediately follow this chunk metadata section.
  /// Not available in wrapped mode.
  pub compressed_body_size: usize,
  /// *How* the chunk body was compressed.
  pub bin_metadata: BinMetadata<T>,
}

fn parse_bins<T: NumberLike>(
  reader: &mut BitReader,
  flags: &Flags,
  n: usize,
) -> QCompressResult<Vec<Bin<T>>> {
  let n_bins = reader.read_usize(BITS_TO_ENCODE_N_BINS)?;
  let mut bins = Vec::with_capacity(n_bins);
  let bits_to_encode_count = flags.bits_to_encode_count(n);
  let maybe_common_gcd = if reader.read_one()? {
    Some(reader.read_uint::<T::Unsigned>(T::Unsigned::BITS)?)
  } else {
    None
  };
  let offset_bits_bits = bits::bits_to_encode_offset_bits::<T::Unsigned>();
  for _ in 0..n_bins {
    let count = reader.read_usize(bits_to_encode_count)?;
    let lower = T::read_from(reader)?;

    let offset_bits = reader.read_bitlen(offset_bits_bits)?;
    if offset_bits > T::Unsigned::BITS {
      return Err(QCompressError::corruption(format!(
        "offset bits of {} exceeds data type of {} bits",
        offset_bits,
        T::Unsigned::BITS,
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
      T::Unsigned::ONE
    } else if let Some(common_gcd) = maybe_common_gcd {
      common_gcd
    } else {
      reader.read_uint(T::Unsigned::BITS)?
    };
    bins.push(Bin {
      count,
      code,
      code_len,
      lower,
      offset_bits,
      run_len_jumpstart,
      gcd,
    });
  }
  Ok(bins)
}

fn write_bins<T: NumberLike>(bins: &[Bin<T>], writer: &mut BitWriter, flags: &Flags, n: usize) {
  writer.write_usize(bins.len(), BITS_TO_ENCODE_N_BINS);
  let bits_to_encode_count = flags.bits_to_encode_count(n);
  let maybe_common_gcd = gcd_utils::common_gcd_for_chunk_meta(bins);
  writer.write_one(maybe_common_gcd.is_some());
  if let Some(common_gcd) = maybe_common_gcd {
    writer.write_diff(common_gcd, T::Unsigned::BITS);
  }
  let offset_bits_bits = bits::bits_to_encode_offset_bits::<T::Unsigned>();
  for bin in bins {
    writer.write_usize(bin.count, bits_to_encode_count);
    bin.lower.write_to(writer);
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
      writer.write_diff(bin.gcd, T::Unsigned::BITS);
    }
  }
}

impl<T: NumberLike> ChunkMetadata<T> {
  pub(crate) fn new(n: usize, bin_metadata: BinMetadata<T>) -> Self {
    ChunkMetadata {
      n,
      compressed_body_size: 0,
      bin_metadata,
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

    let bin_metadata = if flags.delta_encoding_order == 0 {
      let bins = parse_bins::<T>(reader, flags, n)?;
      BinMetadata::Simple { bins }
    } else {
      let bins = parse_bins::<T::Unsigned>(reader, flags, n)?;
      BinMetadata::Delta { bins }
    };

    reader.drain_empty_byte("nonzero bits in end of final byte of chunk metadata")?;

    Ok(Self {
      n,
      compressed_body_size,
      bin_metadata,
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
    match &self.bin_metadata {
      BinMetadata::Simple { bins } => {
        write_bins(bins, writer, flags, self.n);
      }
      BinMetadata::Delta { bins } => {
        write_bins(bins, writer, flags, self.n);
      }
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
