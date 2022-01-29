use std::cmp::{max, min};
use std::fmt;
use std::fmt::Debug;
use std::marker::PhantomData;

use crate::{bits, Flags};
use crate::bit_reader::BitReader;
use crate::chunk_metadata::{ChunkMetadata, DecompressedChunk};
use crate::constants::*;
use crate::errors::{QCompressError, QCompressResult};
use crate::prefix::{Prefix, PrefixDecompressionInfo};
use crate::types::{NumberLike, UnsignedLike};
use crate::utils;

#[derive(Clone, Debug, Default)]
pub struct DecompressorConfig {}

fn validate_prefix_tree<T: NumberLike>(prefixes: &[Prefix<T>], max_depth: u32) -> QCompressResult<()> {
  if prefixes.is_empty() {
    return Ok(());
  }

  let max_n_leafs = 1_usize << max_depth;
  let mut is_specifieds = vec![false; max_n_leafs];
  for p in prefixes {
    let base_idx = bits::bits_to_usize_truncated(&p.val, max_depth);
    let n_leafs = 1_usize << (max_depth - p.val.len() as u32);
    for is_specified in is_specifieds.iter_mut().skip(base_idx).take(n_leafs) {
      if *is_specified {
        return Err(QCompressError::corruption(format!(
          "multiple prefixes for {} found in chunk metadata",
          bits::bits_to_string(&p.val),
        )));
      }
      *is_specified = true;
    }
  }
  for (idx, is_specified) in is_specifieds.iter().enumerate() {
    if !is_specified {
      let val = bits::usize_truncated_to_bits(idx, max_depth);
      return Err(QCompressError::corruption(format!(
        "no prefixes for {} found in chunk metadata",
        bits::bits_to_string(&val),
      )));
    }
  }
  Ok(())
}

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

    let mut max_depth = 0;
    for p in &prefixes {
      max_depth = max(max_depth, p.val.len() as u32);
    }
    validate_prefix_tree(&prefixes, max_depth)?;

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
      unreachable!("prefixes are corrupt yet not caught before decoding numbers");
    }
  }

  fn decompress_chunk_nums(&self, reader: &mut BitReader) -> Vec<T> {
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
    res
  }

  pub fn decompress_chunk(&self, reader: &mut BitReader) -> QCompressResult<Vec<T>> {
    let start_byte_idx = reader.aligned_byte_ind()?;
    let remaining_bytes = reader.size() - start_byte_idx;
    if remaining_bytes < self.compressed_body_size {
      return Err(QCompressError::invalid_argument(format!(
        "bit reader has only {} bytes remaining but compressed body size is {}",
        remaining_bytes,
        self.compressed_body_size,
      )))
    }

    let res = self.decompress_chunk_nums(reader);

    reader.drain_byte();
    let end_byte_idx = reader.aligned_byte_ind()?;
    let real_compressed_body_size = end_byte_idx - start_byte_idx;
    if self.compressed_body_size != real_compressed_body_size {
      return Err(QCompressError::corruption(format!(
        "expected the compressed body to contain {} bytes but it contained {}",
        self.compressed_body_size,
        real_compressed_body_size,
      )));
    }

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
  pub phantom: PhantomData<T>,
}

impl<T> Decompressor<T> where T: NumberLike {
  pub fn from_config(config: DecompressorConfig) -> Self {
    Self {
      config,
      ..Default::default()
    }
  }

  pub fn header(&self, reader: &mut BitReader) -> QCompressResult<Flags> {
    let bytes = reader.read_aligned_bytes(MAGIC_HEADER.len())?;
    if bytes != MAGIC_HEADER {
      return Err(QCompressError::corruption(format!(
        "magic header does not match {:?}; instead found {:?}",
        MAGIC_HEADER,
        bytes,
      )));
    }
    let bytes = reader.read_aligned_bytes(1)?;
    let byte = bytes[0];
    if byte != T::HEADER_BYTE {
      return Err(QCompressError::corruption(format!(
        "data type byte does not match {:?}; instead found {:?}",
        T::HEADER_BYTE,
        byte,
      )));
    }

    Flags::parse_from(reader)
  }

  pub fn chunk_metadata(&self, reader: &mut BitReader, _flags: &Flags) -> QCompressResult<Option<ChunkMetadata<T>>> {
    let magic_byte = reader.read_aligned_bytes(1)?[0];
    if magic_byte == MAGIC_TERMINATION_BYTE {
      return Ok(None);
    } else if magic_byte != MAGIC_CHUNK_BYTE {
      return Err(QCompressError::corruption(format!(
        "invalid magic chunk byte: {}",
        magic_byte
      )));
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
    flags: &Flags,
  ) -> QCompressResult<Vec<T>> {
    let chunk_decompressor = ChunkDecompressor::new(
      metadata,
      self.config.clone(),
      flags.clone(),
    )?;
    chunk_decompressor.decompress_chunk(reader)
  }

  pub fn decompress_chunk(
    &self,
    reader: &mut BitReader,
    flags: &Flags,
  ) -> QCompressResult<Option<DecompressedChunk<T>>> {
    let maybe_metadata = self.chunk_metadata(reader, flags)?;
    match maybe_metadata {
      Some(metadata) => {
        let nums = self.decompress_chunk_body(
          reader,
          metadata.clone(),
          flags,
        )?;
        Ok(Some(DecompressedChunk {
          metadata,
          nums,
        }))
      },
      None => Ok(None)
    }
  }

  pub fn simple_decompress(&self, bytes: Vec<u8>) -> QCompressResult<Vec<T>> {
    // cloning/extending by a single chunk's numbers can slow down by 2%
    // so we just take ownership of the first chunk's numbers instead
    let mut reader = BitReader::from(bytes);
    let mut res: Option<Vec<T>> = None;
    let flags = self.header(&mut reader)?;
    while let Some(chunk) = self.decompress_chunk(&mut reader, &flags)? {
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
    Ok(res.unwrap_or_default())
  }
}

#[cfg(test)]
mod tests {
  use crate::{Decompressor, BitReader, Flags, ChunkMetadata};
  use crate::prefix::Prefix;
  use crate::errors::ErrorKind;

  #[test]
  fn test_corrupt_prefixes_error_not_panic() {
    let decompressor = Decompressor::<i64>::default();
    let bytes = vec![1, 2, 3, 4, 5, 6]; // not important for test

    let metadata_missing_prefix = ChunkMetadata::<i64> {
      n: 2,
      compressed_body_size: 1,
      prefixes: vec![
        Prefix::new(1, vec![false], 100, 100, None),
        Prefix::new(1, vec![true, false], 200, 200, None),
      ]
    };
    let metadata_duplicating_prefix = ChunkMetadata::<i64> {
      n: 2,
      compressed_body_size: 1,
      prefixes: vec![
        Prefix::new(1, vec![false], 100, 100, None),
        Prefix::new(1, vec![false], 200, 200, None),
        Prefix::new(1, vec![true], 300, 300, None),
      ]
    };

    for bad_metadata in vec![metadata_missing_prefix, metadata_duplicating_prefix] {
      let result = decompressor.decompress_chunk_body(
        &mut BitReader::from(bytes.clone()),
        bad_metadata,
        &Flags::default(),
      );
      assert!(matches!(result.unwrap_err().kind, ErrorKind::Corruption));
    }
  }
}

