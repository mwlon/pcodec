use crate::{BitReader, BitWriter, Flags};
use crate::constants::*;
use crate::delta_encoding::DeltaMoments;
use crate::prefix::Prefix;
use crate::types::NumberLike;

#[derive(Clone, Debug)]
pub enum PrefixInfo<T: NumberLike> {
  Simple {
    prefixes: Vec<Prefix<T>>,
  },
  Delta {
    prefixes: Vec<Prefix<T::Signed>>,
    delta_moments: DeltaMoments<T>,
  }
}

#[derive(Clone, Debug)]
pub struct ChunkMetadata<T> where T: NumberLike {
  pub n: usize,
  pub compressed_body_size: usize,
  pub prefix_info: PrefixInfo<T>,
}

fn parse_prefixes<T: NumberLike>(reader: &mut BitReader, flags: &Flags) -> Vec<Prefix<T>> {
  let n_pref = reader.read_usize(MAX_COMPRESSION_LEVEL as usize);
  let mut prefixes = Vec::with_capacity(n_pref);
  let bits_to_encode_prefix_len = flags.bits_to_encode_prefix_len();
  for _ in 0..n_pref {
    let count = reader.read_usize(BITS_TO_ENCODE_N_ENTRIES as usize);
    let lower = T::read_from(reader);
    let upper = T::read_from(reader);
    let code_len = reader.read_usize(bits_to_encode_prefix_len);
    let val = reader.read(code_len);
    let jumpstart = if reader.read_one() {
      Some(reader.read_usize(BITS_TO_ENCODE_JUMPSTART as usize))
    } else {
      None
    };
    prefixes.push(Prefix::new(count, val, lower, upper, jumpstart));
  }
  prefixes
}

fn write_prefixes<T: NumberLike>(prefixes: &[Prefix<T>], writer: &mut BitWriter, flags: &Flags) {
  writer.write_usize(prefixes.len(), MAX_COMPRESSION_LEVEL as usize);
  let bits_to_encode_prefix_len = flags.bits_to_encode_prefix_len();
  for pref in prefixes {
    writer.write_usize(pref.count, BITS_TO_ENCODE_N_ENTRIES as usize);
    pref.lower.write_to(writer);
    pref.upper.write_to(writer);
    writer.write_usize(pref.val.len(), bits_to_encode_prefix_len);
    writer.write_bits(&pref.val);
    match pref.run_len_jumpstart {
      None => {
        writer.write_one(false);
      },
      Some(jumpstart) => {
        writer.write_one(true);
        writer.write_usize(jumpstart, BITS_TO_ENCODE_JUMPSTART as usize);
      },
    }
  }
}

impl<T> ChunkMetadata<T> where T: NumberLike {
  pub fn parse_from(reader: &mut BitReader, flags: &Flags) -> Self {
    let n = reader.read_usize(BITS_TO_ENCODE_N_ENTRIES as usize);
    let compressed_body_size = reader.read_usize(BITS_TO_ENCODE_COMPRESSED_BODY_SIZE as usize);
    let prefix_info = if flags.delta_encoding_order == 0 {
      let prefixes = parse_prefixes::<T>(reader, flags);
      PrefixInfo::Simple {
        prefixes,
      }
    } else {
      let delta_moments = DeltaMoments::<T>::parse_from(reader, flags.delta_encoding_order);
      let prefixes = parse_prefixes::<T::Signed>(reader, flags);
      PrefixInfo::Delta {
        prefixes,
        delta_moments,
      }
    };

    Self {
      n,
      compressed_body_size,
      prefix_info,
    }
  }

  pub fn write_to(&self, writer: &mut BitWriter, flags: &Flags) {
    writer.write_usize(self.n, BITS_TO_ENCODE_N_ENTRIES as usize);
    writer.write_usize(self.compressed_body_size, BITS_TO_ENCODE_COMPRESSED_BODY_SIZE as usize);
    match &self.prefix_info {
      PrefixInfo::Simple { prefixes} => {
        write_prefixes(prefixes, writer, flags);
      },
      PrefixInfo::Delta { prefixes, delta_moments } => {
        delta_moments.write_to(writer);
        write_prefixes(prefixes, writer, flags);
      },
    }
    writer.finish_byte();
  }

  pub fn update_write_compressed_body_size(&self, writer: &mut BitWriter, initial_idx: usize) {
    writer.assign_usize(
      initial_idx + BITS_TO_ENCODE_N_ENTRIES as usize / 8,
      BITS_TO_ENCODE_N_ENTRIES as usize % 8,
      self.compressed_body_size,
      BITS_TO_ENCODE_COMPRESSED_BODY_SIZE,
    );
  }
}

#[derive(Clone)]
pub struct DecompressedChunk<T> where T: NumberLike {
  pub metadata: ChunkMetadata<T>,
  pub nums: Vec<T>,
}
