use std::cmp::{max, min};
use std::fmt;
use std::fmt::Debug;
use std::marker::PhantomData;

use crate::bit_reader::BitReader;
use crate::{bits, Flags};
use crate::chunk_metadata::{ChunkMetadata, DecompressedChunk};
use crate::constants::*;
use crate::errors::{QCompressError, QCompressResult};
use crate::prefix::{Prefix, PrefixDecompressionInfo};
use crate::types::{NumberLike, UnsignedLike};
use crate::utils;

#[derive(Clone, Debug, Default)]
pub struct DecompressorConfig {}

#[derive(Clone)]
struct ChunkDecompressor<T> where T: NumberLike {
  prefixes: Vec<Prefix<T>>,
  prefix_map: Vec<PrefixDecompressionInfo<T::Unsigned>>,
  prefix_len_map: Vec<u32>,
  max_depth: u32,
  n: usize,
  is_single_prefix: bool,
  compressed_body_size: usize,
}

impl<T> ChunkDecompressor<T> where T: NumberLike {
  pub fn new(
    metadata: ChunkMetadata<T>,
    _config: DecompressorConfig,
    _flags: Flags,
  ) -> QCompressResult<Self> {
    let ChunkMetadata {
      n,
      prefixes,
      ..
    } = metadata;

    // TODO validate prefixes exactly produce a binary tree

    let mut max_depth = 0;
    for p in &prefixes {
      max_depth = max(max_depth, p.val.len() as u32);
    }
    let n_pref = 1_usize << max_depth;
    let mut prefix_map = Vec::with_capacity(n_pref);
    let mut prefix_len_map = Vec::with_capacity(n_pref);
    for _ in 0..n_pref {
      prefix_map.push(PrefixDecompressionInfo::new());
      prefix_len_map.push(u32::MAX);
    }
    for p in &prefixes {
      let i = bits::bits_to_usize_truncated(&p.val, max_depth);
      prefix_map[i] = p.into();
      prefix_len_map[i] = p.val.len() as u32;
    }

    let is_single_prefix = prefixes.len() == 1;
    Ok(ChunkDecompressor {
      prefixes,
      prefix_map,
      prefix_len_map,
      max_depth,
      n,
      is_single_prefix,
      compressed_body_size: metadata.compressed_body_size,
    })
  }

  fn next_prefix(&self, reader: &mut BitReader) -> PrefixDecompressionInfo<T::Unsigned> {
    if self.is_single_prefix {
      self.prefix_map[0]
    } else {
      let mut prefix_idx = 0;
      for prefix_len in 1..self.max_depth + 1 {
        if reader.read_one() {
          prefix_idx |= 1 << (self.max_depth - prefix_len);
        }
        if self.prefix_len_map[prefix_idx] == prefix_len {
          return self.prefix_map[prefix_idx];
        }
      }
      panic!("prefixes are corrupt");
    }
  }

  pub fn decompress_chunk(&self, reader: &mut BitReader) -> QCompressResult<Vec<T>> {
    let (start_byte_idx, _) = reader.inds();
    let n = self.n;
    let mut res = Vec::with_capacity(n);
    let mut i = 0;
    while i < n {
      let p = self.next_prefix(reader);

      let reps = match p.run_len_jumpstart {
        None => {
          1
        },
        Some(jumpstart) => {
          // we stored the number of occurrences minus 1
          // because we knew it's at least 1
          min(reader.read_varint(jumpstart) + 1, n - i)
        },
      };

      for _ in 0..reps {
        let mut offset = reader.read_diff(p.k as usize);
        if p.k < T::Unsigned::BITS {
          let most_significant = T::Unsigned::ONE << p.k;
          if p.range - offset >= most_significant && reader.read_one() {
            offset |= most_significant;
          }
        }
        let num = T::from_unsigned(p.lower_unsigned + offset);
        res.push(num);
      }
      i += reps;
    }

    let (end_byte_idx, _) = reader.inds();
    let real_compressed_body_size = end_byte_idx - start_byte_idx;
    if self.compressed_body_size != real_compressed_body_size {
      return Err(QCompressError::CompressedBodySize {
        expected: self.compressed_body_size,
        actual: real_compressed_body_size,
      });
    }

    reader.drain_byte();
    Ok(res)
  }
}

impl<T> Debug for ChunkDecompressor<T> where T: NumberLike {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    utils::display_prefixes(&self.prefixes, f)
  }
}

#[derive(Clone, Debug, Default)]
pub struct Decompressor<T> where T: NumberLike {
  pub config: DecompressorConfig,
  maybe_flags: Option<Flags>,
  phantom: PhantomData<T>,
}

impl<T> Decompressor<T> where T: NumberLike {
  pub fn new() -> Self {
    Self::default()
  }

  pub fn from_config(config: DecompressorConfig) -> Self {
    Self {
      config,
      ..Default::default()
    }
  }

  fn apply_flags(&mut self, reader: &mut BitReader) -> QCompressResult<Flags> {
    let flags = Flags::parse_from(reader)?;
    self.maybe_flags = Some(flags.clone());
    Ok(flags)
  }

  pub fn apply_header(&mut self, reader: &mut BitReader) -> QCompressResult<Flags> {
    let bytes = reader.read_bytes(MAGIC_HEADER.len())?;
    if bytes != MAGIC_HEADER {
      return Err(QCompressError::MagicHeaderError {
        header: bytes.to_vec()
      });
    }
    let bytes = reader.read_bytes(1)?;
    let byte = bytes[0];
    if byte != T::HEADER_BYTE {
      return Err(QCompressError::HeaderDtypeError {
        header_byte: byte,
        decompressor_byte: T::HEADER_BYTE,
      });
    }

    self.apply_flags(reader)
  }

  pub fn chunk_metadata(&self, reader: &mut BitReader) -> QCompressResult<Option<ChunkMetadata<T>>> {
    let magic_byte = reader.read_bytes(1)?[0];
    if magic_byte == MAGIC_TERMINATION_BYTE {
      return Ok(None);
    } else if magic_byte != MAGIC_CHUNK_BYTE {
      return Err(QCompressError::MagicChunkByteError { byte: magic_byte });
    }

    // otherwise there is indeed another chunk
    let n = reader.read_usize(BITS_TO_ENCODE_N_ENTRIES as usize);
    let compressed_body_size = reader.read_usize(BITS_TO_ENCODE_COMPRESSED_BODY_SIZE as usize);
    let n_pref = reader.read_usize(MAX_MAX_DEPTH as usize);
    let mut prefixes = Vec::with_capacity(n_pref);
    for _ in 0..n_pref {
      let count = reader.read_usize(BITS_TO_ENCODE_N_ENTRIES as usize);
      let lower_bits = reader.read(T::PHYSICAL_BITS);
      let lower = T::from_bytes(bits::bits_to_bytes(lower_bits));
      let upper_bits = reader.read(T::PHYSICAL_BITS);
      let upper = T::from_bytes(bits::bits_to_bytes(upper_bits));
      let code_len = reader.read_usize(BITS_TO_ENCODE_PREFIX_LEN as usize);
      let val = reader.read(code_len);
      let jumpstart = if reader.read_one() {
        Some(reader.read_usize(BITS_TO_ENCODE_JUMPSTART as usize))
      } else {
        None
      };
      prefixes.push(Prefix::new(count, val, lower, upper, jumpstart));
    }
    reader.drain_byte();

    Ok(Some(ChunkMetadata {
      n,
      compressed_body_size,
      prefixes,
    }))
  }

  pub fn decompress_chunk_body(
    &self,
    reader: &mut BitReader,
    metadata: ChunkMetadata<T>,
  ) -> QCompressResult<Vec<T>> {
    if let Some(flags) = self.maybe_flags.clone() {
      let chunk_decompressor = ChunkDecompressor::new(
        metadata,
        self.config.clone(),
        flags,
      )?;
      chunk_decompressor.decompress_chunk(reader)
    } else {
      Err(QCompressError::UninitializedError)
    }
  }

  pub fn decompress_chunk(&self, reader: &mut BitReader) -> QCompressResult<Option<DecompressedChunk<T>>> {
    let maybe_metadata = self.chunk_metadata(reader)?;
    match maybe_metadata {
      Some(metadata) => {
        let nums = self.decompress_chunk_body(
          reader,
          metadata.clone(),
        )?;
        Ok(Some(DecompressedChunk {
          metadata,
          nums,
        }))
      },
      None => Ok(None)
    }
  }

  pub fn simple_decompress(&mut self, reader: &mut BitReader) -> QCompressResult<Vec<T>> {
    // cloning/extending by a single chunk's numbers can slow down by 2%
    // so we just take ownership of the first chunk's numbers instead
    let mut res: Option<Vec<T>> = None;
    self.apply_header(reader)?;
    while let Some(chunk) = self.decompress_chunk(reader)? {
      res = match res {
        Some(mut existing) => {
          existing.extend(chunk.nums);
          Some(existing)
        }
        None => {
          Some(chunk.nums)
        }
      };
    }
    Ok(res.unwrap_or(vec![]))
  }
}

