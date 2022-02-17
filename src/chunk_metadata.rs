use crate::{BitReader, BitWriter, Flags};
use crate::constants::*;
use crate::delta_encoding::DeltaMoments;
use crate::prefix::Prefix;
use crate::data_types::NumberLike;
use crate::errors::{QCompressResult, QCompressError};

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
  ///
  /// `Delta` prefix info also contains a `Vec` of initial delta moments at
  /// the start of the chunk, each of which is also a `SignedLike`.
  Delta {
    prefixes: Vec<Prefix<T::Signed>>,
    delta_moments: DeltaMoments<T>,
  }
}

/// The metadata of a .qco file chunk.
///
/// Each file may contain multiple metadata sections, so to count the
/// entries, one must sum the count `n` for each chunk metadata. This can
/// be done easily - see the fast_seeking.rs example.
/// One can also create a rough histogram (or a histogram of deltas, if
/// delta encoding was used) by aggregating chunk metadata.
#[derive(Clone, Debug, PartialEq)]
pub struct ChunkMetadata<T> where T: NumberLike {
  /// The count of numbers in the chunk.
  pub n: usize,
  /// The compressed byte length of the compressed numbers that immediately
  /// follow this chunk metadata section.
  pub compressed_body_size: usize,
  /// *How* the chunk body was compressed.
  pub prefix_metadata: PrefixMetadata<T>,
}

fn parse_prefixes<T: NumberLike>(
  reader: &mut BitReader,
  flags: &Flags,
) -> QCompressResult<Vec<Prefix<T>>> {
  let n_pref = reader.read_usize(BITS_TO_ENCODE_N_PREFIXES)?;
  let mut prefixes = Vec::with_capacity(n_pref);
  let bits_to_encode_prefix_len = flags.bits_to_encode_prefix_len();
  for _ in 0..n_pref {
    let count = reader.read_usize(BITS_TO_ENCODE_N_ENTRIES)?;
    let lower = T::read_from(reader)?;
    let upper = T::read_from(reader)?;

    if lower.gt(&upper) {
      return Err(QCompressError::corruption(format!(
        "prefix lower bound {} may not be greater than upper bound {}",
        lower,
        upper,
      )));
    }

    let code_len = reader.read_usize(bits_to_encode_prefix_len)?;
    let code = reader.read(code_len)?;
    let run_len_jumpstart = if reader.read_one()? {
      Some(reader.read_usize(BITS_TO_ENCODE_JUMPSTART)?)
    } else {
      None
    };
    prefixes.push(Prefix {
      count,
      code,
      lower,
      upper,
      run_len_jumpstart,
    });
  }
  Ok(prefixes)
}

fn write_prefixes<T: NumberLike>(prefixes: &[Prefix<T>], writer: &mut BitWriter, flags: &Flags) {
  writer.write_usize(prefixes.len(), BITS_TO_ENCODE_N_PREFIXES);
  let bits_to_encode_prefix_len = flags.bits_to_encode_prefix_len();
  for pref in prefixes {
    writer.write_usize(pref.count, BITS_TO_ENCODE_N_ENTRIES);
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
  }
}

impl<T> ChunkMetadata<T> where T: NumberLike {
  pub fn parse_from(reader: &mut BitReader, flags: &Flags) -> QCompressResult<Self> {
    let n = reader.read_usize(BITS_TO_ENCODE_N_ENTRIES)?;
    let compressed_body_size = reader.read_usize(BITS_TO_ENCODE_COMPRESSED_BODY_SIZE)?;
    let prefix_metadata = if flags.delta_encoding_order == 0 {
      let prefixes = parse_prefixes::<T>(reader, flags)?;
      PrefixMetadata::Simple {
        prefixes,
      }
    } else {
      let delta_moments = DeltaMoments::<T>::parse_from(reader, flags.delta_encoding_order)?;
      let prefixes = parse_prefixes::<T::Signed>(reader, flags)?;
      PrefixMetadata::Delta {
        prefixes,
        delta_moments,
      }
    };

    Ok(Self {
      n,
      compressed_body_size,
      prefix_metadata,
    })
  }

  pub fn write_to(&self, writer: &mut BitWriter, flags: &Flags) {
    writer.write_usize(self.n, BITS_TO_ENCODE_N_ENTRIES);
    writer.write_usize(self.compressed_body_size, BITS_TO_ENCODE_COMPRESSED_BODY_SIZE);
    match &self.prefix_metadata {
      PrefixMetadata::Simple { prefixes} => {
        write_prefixes(prefixes, writer, flags);
      },
      PrefixMetadata::Delta { prefixes, delta_moments } => {
        delta_moments.write_to(writer);
        write_prefixes(prefixes, writer, flags);
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
      bit_idx + BITS_TO_ENCODE_N_ENTRIES,
      self.compressed_body_size,
      BITS_TO_ENCODE_COMPRESSED_BODY_SIZE,
    );
  }
}

/// A whole chunk of decompressed data - both the metadata
/// and the numbers it contained.
#[derive(Clone)]
pub struct DecompressedChunk<T> where T: NumberLike {
  pub metadata: ChunkMetadata<T>,
  pub nums: Vec<T>,
}
