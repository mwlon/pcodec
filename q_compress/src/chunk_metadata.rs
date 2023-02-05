use crate::{Flags, gcd_utils};
use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::constants::*;
use crate::data_types::{NumberLike, UnsignedLike};
use crate::delta_encoding::DeltaMoments;
use crate::errors::{QCompressError, QCompressResult};
use crate::prefix::Prefix;

// TODO in 1.0 make this more non_exhaustive
/// A wrapper for prefixes in the two cases cases: delta encoded or not.
/// 
/// This is the part of chunk metadata that describes *how* the data was
/// compressed - the Huffman codes used and what ranges they specify.
#[derive(Clone, Debug, PartialEq)]
pub enum PrefixMetadata<T: NumberLike> {
  /// `Simple` prefix metadata corresponds to the case when delta encoding is
  /// off (`delta_encoding_order` of 0).
  ///
  /// It simply contains prefixes of the same data type being compressed.
  Simple {
    prefixes: Vec<Prefix<T>>,
  },
  /// `Delta` prefix metadata corresponds to the case when delta encoding is
  /// on.
  ///
  /// It contains prefixes of the associated `SignedLike` type (what the
  /// deltas are expressed as). For instance, a chunk of delta-encoded `f64`s
  /// with `delta_encoding_order: 1`
  /// will have prefixes of type `i64`, where a delta of n indicates a change
  /// of n * machine epsilon from the last float.
  #[non_exhaustive]
  Delta {
    prefixes: Vec<Prefix<T::Signed>>,
  }
}

impl<T: NumberLike> PrefixMetadata<T> {
  pub(crate) fn use_gcd(&self) -> bool {
    match self {
      PrefixMetadata::Simple { prefixes } => gcd_utils::use_gcd_arithmetic(prefixes),
      PrefixMetadata::Delta { prefixes } => gcd_utils::use_gcd_arithmetic(prefixes),
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
pub struct ChunkMetadata<T> where T: NumberLike {
  /// The count of numbers in the chunk.
  /// Not available in wrapped mode.
  pub n: usize,
  /// The compressed byte length of the body that immediately follow this chunk metadata section.
  /// Not available in wrapped mode.
  pub compressed_body_size: usize,
  /// *How* the chunk body was compressed.
  pub prefix_metadata: PrefixMetadata<T>,
  // not available in wrapped mode
  pub(crate) delta_moments: DeltaMoments<T::Signed>,
}

fn parse_prefixes<T: NumberLike>(
  reader: &mut BitReader,
  flags: &Flags,
  n: usize,
) -> QCompressResult<Vec<Prefix<T>>> {
  let n_pref = reader.read_usize(BITS_TO_ENCODE_N_PREFIXES)?;
  let mut prefixes = Vec::with_capacity(n_pref);
  let bits_to_encode_code_len = flags.bits_to_encode_code_len();
  let bits_to_encode_count = flags.bits_to_encode_count(n);
  let maybe_common_gcd = if flags.use_gcds {
    if reader.read_one()? {
      Some(gcd_utils::read_gcd(T::Unsigned::MAX, reader)?)
    } else {
      None
    }
  } else {
    Some(T::Unsigned::ONE)
  };
  for _ in 0..n_pref {
    let count = reader.read_usize(bits_to_encode_count)?;
    let lower = T::read_from(reader)?;
    let upper = T::read_from(reader)?;

    let lower_u = lower.to_unsigned();
    let upper_u = upper.to_unsigned();
    if lower_u > upper_u {
      return Err(QCompressError::corruption(format!(
        "prefix lower bound {} may not be greater than upper bound {}",
        lower,
        upper,
      )));
    }

    let code_len = reader.read_usize(bits_to_encode_code_len)?;
    let code = reader.read(code_len)?;
    let run_len_jumpstart = if reader.read_one()? {
      Some(reader.read_usize(BITS_TO_ENCODE_JUMPSTART)?)
    } else {
      None
    };
    let gcd = if let Some(common_gcd) = maybe_common_gcd {
      common_gcd
    } else {
      gcd_utils::read_gcd(upper_u - lower_u, reader)?
    };
    prefixes.push(Prefix {
      count,
      code,
      lower,
      upper,
      run_len_jumpstart,
      gcd,
    });
  }
  Ok(prefixes)
}

fn write_prefixes<T: NumberLike>(
  prefixes: &[Prefix<T>],
  writer: &mut BitWriter,
  flags: &Flags,
  n: usize,
) {
  writer.write_usize(prefixes.len(), BITS_TO_ENCODE_N_PREFIXES);
  let bits_to_encode_prefix_len = flags.bits_to_encode_code_len();
  let bits_to_encode_count = flags.bits_to_encode_count(n);
  let maybe_commond_gcd = if flags.use_gcds {
    let maybe_common_gcd = gcd_utils::common_gcd_for_chunk_meta(prefixes);
    writer.write_one(maybe_common_gcd.is_some());
    if let Some(common_gcd) = maybe_common_gcd {
      gcd_utils::write_gcd(T::Unsigned::MAX, common_gcd, writer);
    }
    maybe_common_gcd
  } else {
    Some(T::Unsigned::ONE)
  };
  for pref in prefixes {
    writer.write_usize(pref.count, bits_to_encode_count);
    pref.lower.write_to(writer);
    pref.upper.write_to(writer);
    writer.write_usize(pref.code.len(), bits_to_encode_prefix_len);
    writer.write(&pref.code);
    match pref.run_len_jumpstart {
      None => {
        writer.write_one(false);
      },
      Some(jumpstart) => {
        writer.write_one(true);
        writer.write_usize(jumpstart, BITS_TO_ENCODE_JUMPSTART);
      },
    }
    if maybe_commond_gcd.is_none() {
      gcd_utils::write_gcd(pref.upper.to_unsigned() - pref.lower.to_unsigned(), pref.gcd, writer);
    }
  }
}

impl<T> ChunkMetadata<T> where T: NumberLike {
  pub(crate) fn new(n: usize, prefix_metadata: PrefixMetadata<T>, delta_moments: DeltaMoments<T::Signed>) -> Self {
    ChunkMetadata {
      n,
      compressed_body_size: 0,
      prefix_metadata,
      delta_moments,
    }
  }

  // TODO in 1.0 make this private
  pub fn parse_from(reader: &mut BitReader, flags: &Flags) -> QCompressResult<Self> {
    let (n, compressed_body_size, delta_moments) = if flags.use_wrapped_mode {
      (
        0,
        0,
        DeltaMoments::default(),
      )
    } else {
      (
        reader.read_usize(BITS_TO_ENCODE_N_ENTRIES)?,
        reader.read_usize(BITS_TO_ENCODE_COMPRESSED_BODY_SIZE)?,
        DeltaMoments::<T::Signed>::parse_from(reader, flags.delta_encoding_order)?,
      )
    };

    let prefix_metadata = if flags.delta_encoding_order == 0 {
      let prefixes = parse_prefixes::<T>(reader, flags, n)?;
      PrefixMetadata::Simple {
        prefixes,
      }
    } else {
      let prefixes = parse_prefixes::<T::Signed>(reader, flags, n)?;
      PrefixMetadata::Delta {
        prefixes,
      }
    };

    reader.drain_empty_byte("nonzero bits in end of final byte of chunk metadata")?;

    Ok(Self {
      n,
      compressed_body_size,
      prefix_metadata,
      delta_moments,
    })
  }

  // TODO in 1.0 make this private
  pub fn write_to(&self, writer: &mut BitWriter, flags: &Flags) {
    if !flags.use_wrapped_mode {
      writer.write_usize(self.n, BITS_TO_ENCODE_N_ENTRIES);
      writer.write_usize(self.compressed_body_size, BITS_TO_ENCODE_COMPRESSED_BODY_SIZE);
      self.delta_moments.write_to(writer);
    }
    match &self.prefix_metadata {
      PrefixMetadata::Simple { prefixes} => {
        write_prefixes(prefixes, writer, flags, self.n);
      },
      PrefixMetadata::Delta { prefixes } => {
        write_prefixes(prefixes, writer, flags, self.n);
      },
    }
    writer.finish_byte();
  }

  pub(crate) fn update_write_compressed_body_size(
    &self,
    writer: &mut BitWriter,
    bit_idx: usize,
  ) {
    writer.overwrite_usize(
      bit_idx + BITS_TO_ENCODE_N_ENTRIES + 8,
      self.compressed_body_size,
      BITS_TO_ENCODE_COMPRESSED_BODY_SIZE,
    );
  }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum DataPagingSpec {
  SinglePage,
  ExactPageSizes(Vec<usize>),
}

impl Default for DataPagingSpec {
  fn default() -> Self {
    DataPagingSpec::SinglePage
  }
}
