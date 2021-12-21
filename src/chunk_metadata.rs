use crate::types::NumberLike;
use crate::prefix::Prefix;
use crate::constants::*;
use crate::bits::{bits_to_bytes, bytes_to_bits, usize_to_bits};
use crate::BitReader;
use crate::errors::QCompressResult;

#[derive(Clone, Debug)]
pub struct ChunkMetadata<T> where T: NumberLike {
  pub n: usize,
  pub compressed_body_size: usize,
  pub prefixes: Vec<Prefix<T>>,
}

impl<T> ChunkMetadata<T> where T: NumberLike {
  pub fn parse_from(reader: &mut BitReader) -> QCompressResult<Self> {
    let n = reader.read_usize(BITS_TO_ENCODE_N_ENTRIES as usize);
    let compressed_body_size = reader.read_usize(BITS_TO_ENCODE_COMPRESSED_BODY_SIZE as usize);
    let n_pref = reader.read_usize(MAX_MAX_DEPTH as usize);
    let mut prefixes = Vec::with_capacity(n_pref);
    for _ in 0..n_pref {
      let count = reader.read_usize(BITS_TO_ENCODE_N_ENTRIES as usize);
      let lower_bits = reader.read(T::PHYSICAL_BITS);
      let lower = T::from_bytes(bits_to_bytes(lower_bits));
      let upper_bits = reader.read(T::PHYSICAL_BITS);
      let upper = T::from_bytes(bits_to_bytes(upper_bits));
      let code_len = reader.read_usize(BITS_TO_ENCODE_PREFIX_LEN as usize);
      let val = reader.read(code_len);
      let jumpstart = if reader.read_one() {
        Some(reader.read_usize(BITS_TO_ENCODE_JUMPSTART as usize))
      } else {
        None
      };
      prefixes.push(Prefix::new(count, val, lower, upper, jumpstart));
    }

    Ok(Self {
      n,
      compressed_body_size,
      prefixes,
    })
  }

  pub fn to_bytes(&self) -> Vec<u8> {
    let mut res = Vec::new();
    res.extend(usize_to_bits(self.n, BITS_TO_ENCODE_N_ENTRIES));
    res.extend(usize_to_bits(self.compressed_body_size, BITS_TO_ENCODE_COMPRESSED_BODY_SIZE));
    res.extend(usize_to_bits(self.prefixes.len(), MAX_MAX_DEPTH));
    for pref in &self.prefixes {
      res.extend(usize_to_bits(pref.count, BITS_TO_ENCODE_N_ENTRIES));
      res.extend(bytes_to_bits(T::bytes_from(pref.lower)));
      res.extend(bytes_to_bits(T::bytes_from(pref.upper)));
      res.extend(usize_to_bits(pref.val.len(), BITS_TO_ENCODE_PREFIX_LEN));
      res.extend(&pref.val);
      match pref.run_len_jumpstart {
        None => {
          res.push(false);
        },
        Some(jumpstart) => {
          res.push(true);
          res.extend(usize_to_bits(jumpstart, BITS_TO_ENCODE_JUMPSTART))
        },
      }
    }
    bits_to_bytes(res)
  }
}

#[derive(Clone)]
pub struct CompressedChunk<T> where T: NumberLike {
  pub metadata: ChunkMetadata<T>,
  pub bytes: Vec<u8>,
}

#[derive(Clone)]
pub struct DecompressedChunk<T> where T: NumberLike {
  pub metadata: ChunkMetadata<T>,
  pub nums: Vec<T>,
}
