use std::cmp::{max, min};
use std::fmt::Debug;
use std::marker::PhantomData;

use crate::{bits, Flags};
use crate::bit_reader::BitReader;
use crate::chunk_metadata::{ChunkMetadata, DecompressedChunk, PrefixMetadata};
use crate::constants::*;
use crate::data_types::{NumberLike, UnsignedLike};
use crate::delta_encoding;
use crate::errors::{QCompressError, QCompressResult};
use crate::huffman_decoding::HuffmanTable;
use crate::prefix::Prefix;

const UNCHECKED_NUM_THRESHOLD: usize = 30;

/// All the settings you can specify about decompression.
#[derive(Clone, Debug, Default)]
pub struct DecompressorConfig {}

fn validate_prefix_tree<T: NumberLike>(prefixes: &[Prefix<T>]) -> QCompressResult<()> {
  if prefixes.is_empty() {
    return Ok(());
  }

  let mut max_depth = 0;
  for p in prefixes {
    max_depth = max(max_depth, p.code.len());
  }

  let max_n_leafs = 1_usize << max_depth;
  let mut is_specifieds = vec![false; max_n_leafs];
  for p in prefixes {
    let base_idx = bits::bits_to_usize_truncated(&p.code, max_depth);
    let n_leafs = 1_usize << (max_depth - p.code.len());
    for is_specified in is_specifieds.iter_mut().skip(base_idx).take(n_leafs) {
      if *is_specified {
        return Err(QCompressError::corruption(format!(
          "multiple prefixes for {} found in chunk metadata",
          bits::bits_to_string(&p.code),
        )));
      }
      *is_specified = true;
    }
  }
  for (idx, is_specified) in is_specifieds.iter().enumerate() {
    if !is_specified {
      let code = bits::usize_truncated_to_bits(idx, max_depth);
      return Err(QCompressError::corruption(format!(
        "no prefixes for {} found in chunk metadata",
        bits::bits_to_string(&code),
      )));
    }
  }
  Ok(())
}

#[derive(Clone, Debug)]
struct ChunkDecompressor<T> where T: NumberLike {
  huffman_table: HuffmanTable<T::Unsigned>,
  n: usize,
  compressed_body_size: usize,
  max_bits_per_num_block: usize,
}

impl<T> ChunkDecompressor<T> where T: NumberLike {
  pub fn new(
    n: usize,
    compressed_body_size: usize,
    prefixes: Vec<Prefix<T>>,
    _config: DecompressorConfig,
    _flags: Flags,
  ) -> QCompressResult<Self> {
    if prefixes.is_empty() && n > 0 {
      return Err(QCompressError::corruption(format!(
        "unable to decompress chunk with no prefixes and {} numbers",
        n,
      )));
    }
    validate_prefix_tree(&prefixes)?;

    let max_bits_per_num_block = prefixes.iter()
      .map(|p| {
        let prefix_bits = p.code.len();
        let (max_reps, max_jumpstart_bits) = match p.run_len_jumpstart {
          None => (1, 0),
          Some(_) => (MAX_ENTRIES, 2 * BITS_TO_ENCODE_N_ENTRIES),
        };
        let k_info = p.k_info();
        let max_bits_per_offset = if k_info.only_k_bits_lower == T::Unsigned::ZERO {
          k_info.k
        } else {
          k_info.k + 1
        };
        let overshoot_prefix_bits = ((prefix_bits + PREFIX_TABLE_SIZE_LOG - 1)
          / PREFIX_TABLE_SIZE_LOG) * PREFIX_TABLE_SIZE_LOG;

        max(
          prefix_bits + max_jumpstart_bits + max_reps * max_bits_per_offset,
          overshoot_prefix_bits,
        )
      })
      .max()
      .unwrap_or(usize::MAX);

    Ok(ChunkDecompressor {
      huffman_table: HuffmanTable::from(&prefixes),
      n,
      compressed_body_size,
      max_bits_per_num_block,
    })
  }

  fn unchecked_decompress_num_block(
    &self,
    reader: &mut BitReader,
    res: &mut Vec<T>,
  ) {
    let p = self.huffman_table.unchecked_search_with_reader(reader);

    let reps = match p.run_len_jumpstart {
      None => 1,
      // we stored the number of occurrences minus 1 because we knew it's at least 1
      Some(jumpstart) => min(reader.unchecked_read_varint(jumpstart) + 1, self.n - res.len()),
    };

    for _ in 0..reps {
      let mut offset = reader.unchecked_read_diff(p.k);
      if p.k < T::Unsigned::BITS {
        let most_significant = T::Unsigned::ONE << p.k;
        if p.range - offset >= most_significant && reader.unchecked_read_one() {
          offset |= most_significant;
        }
      }
      let num = T::from_unsigned(p.lower_unsigned + offset);
      res.push(num);
    }
  }

  fn decompress_num_block(
    &self,
    reader: &mut BitReader,
    res: &mut Vec<T>,
  ) -> QCompressResult<()> {
    let p = self.huffman_table.search_with_reader(reader)?;

    let reps = match p.run_len_jumpstart {
      None => 1,
      // we stored the number of occurrences minus 1 because we knew it's at least 1
      Some(jumpstart) => min(reader.read_varint(jumpstart)? + 1, self.n - res.len()),
    };

    for _ in 0..reps {
      let mut offset = reader.read_diff(p.k)?;
      if p.k < T::Unsigned::BITS {
        let most_significant = T::Unsigned::ONE << p.k;
        if p.range - offset >= most_significant && reader.read_one()? {
          offset |= most_significant;
        }
      }
      let num = T::from_unsigned(p.lower_unsigned + offset);
      res.push(num);
    }

    Ok(())
  }

  // After much debugging a performance degradation from error handling changes,
  // it turned out this function's logic ran slower when inlining.
  // I don't understand why, but telling it not
  // to inline fixed the performance issue.
  // https://stackoverflow.com/questions/70911460/why-does-an-unrelated-heap-allocation-in-the-same-rust-scope-hurt-performance
  #[inline(never)]
  fn decompress_chunk_nums(&self, reader: &mut BitReader) -> QCompressResult<Vec<T>> {
    let mut res = Vec::with_capacity(self.n);

    if self.max_bits_per_num_block == 0 {
      let mut temp = Vec::with_capacity(1);
      self.unchecked_decompress_num_block(reader, &mut temp);
      let constant_num = temp[0];
      for _ in 0..self.n {
        res.push(constant_num);
      }
      return Ok(res);
    }

    loop {
      let remaining_nums = self.n - res.len();
      let guaranteed_safe_num_blocks = min(
        remaining_nums,
        reader.bits_remaining() / self.max_bits_per_num_block,
      );

      if guaranteed_safe_num_blocks >= UNCHECKED_NUM_THRESHOLD {
        let mut block_idx = 0;
        while block_idx < guaranteed_safe_num_blocks && res.len() < self.n {
          self.unchecked_decompress_num_block(reader, &mut res);
          block_idx += 1;
        }
      } else {
        break;
      }
    }

    while res.len() < self.n {
      self.decompress_num_block(reader, &mut res)?;
    }
    Ok(res)
  }

  fn validate_sufficient_data(&self, reader: &BitReader) -> QCompressResult<()> {
    let start_byte_idx = reader.aligned_byte_ind()?;
    let remaining_bytes = reader.byte_size() - start_byte_idx;
    if remaining_bytes < self.compressed_body_size {
      Err(QCompressError::insufficient_data(format!(
        "bit reader has only {} bytes remaining but compressed body size is {}",
        remaining_bytes,
        self.compressed_body_size,
      )))
    } else {
      Ok(())
    }
  }

  pub fn decompress_chunk_body(&self, reader: &mut BitReader) -> QCompressResult<Vec<T>> {
    // This checks that we have enough data, assuming the file is not corrupt.
    // We still need to be careful in `decompress_chunk_nums`.
    self.validate_sufficient_data(reader)?;

    let start_byte_idx = reader.aligned_byte_ind()?;
    let res = self.decompress_chunk_nums(reader)?;

    reader.drain_empty_byte(|| QCompressError::corruption(
      "nonzero bits in end of final byte of chunk numbers"
    ))?;
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

/// Converts compressed bytes into [`Flags`], [`ChunkMetadata`],
/// and vectors of numbers.
///
/// You can use the decompressor very easily:
/// ```
/// use q_compress::Decompressor;
///
/// let my_bytes = vec![113, 99, 111, 33, 3, 0, 46]; // the simplest possible .qco file; empty
/// let decompressor = Decompressor::<i32>::default();
/// let nums = decompressor.simple_decompress(my_bytes).expect("decompression"); // returns Vec<i32>
/// ```
/// You can also get full control over the decompression process:
/// ```
/// use q_compress::{BitReader, Decompressor};
///
/// let my_bytes = vec![113, 99, 111, 33, 3, 0, 46]; // the simplest possible .qco file; empty
/// let mut reader = BitReader::from(my_bytes);
/// let decompressor = Decompressor::<i32>::default();
///
/// let flags = decompressor.header(&mut reader).expect("header failure");
/// while let Some(chunk_meta) = decompressor.chunk_metadata(&mut reader, &flags).expect("chunk meta failure") {
///   let nums = decompressor.chunk_body(&mut reader, &flags, &chunk_meta).expect("chunk body failure");
/// }
/// // We don't need to explicitly read the footer because `.chunk_metadata()`
/// // returns `None` when it reaches the footer byte.
/// ```
#[derive(Clone, Debug, Default)]
pub struct Decompressor<T> where T: NumberLike {
  config: DecompressorConfig,
  phantom: PhantomData<T>,
}

impl<T> Decompressor<T> where T: NumberLike {
  /// Creates a new decompressor, given a [`DecompressorConfig`].
  /// This config has nothing to do with [`Flags`], which will be parsed out
  /// of a .qco file's header.
  pub fn from_config(config: DecompressorConfig) -> Self {
    Self {
      config,
      ..Default::default()
    }
  }

  /// Reads the header, returning its [`Flags`].
  /// Will return an error if the reader is not byte-aligned,
  /// if the reader runs out of data, if the data type byte does not agree,
  /// if the flags are from a newer, incompatible version of q_compress,
  /// or if any corruptions are found.
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

  /// Reads a [`ChunkMetadata`], returning it.
  /// Will return `None` if it instead finds a termination footer
  /// (indicating end of the .qco file).
  /// Will return an error if the reader is not byte-aligned,
  /// the reader runs out of data, or any corruptions are found.
  ///
  /// Typically one would pass in the [`Flags`] obtained from an earlier
  /// [`.header()`][Self::header] call.
  pub fn chunk_metadata(&self, reader: &mut BitReader, flags: &Flags) -> QCompressResult<Option<ChunkMetadata<T>>> {
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
    let metadata = ChunkMetadata::parse_from(reader, flags)?;
    reader.drain_empty_byte(|| QCompressError::corruption(
      "nonzero bits in end of final byte of chunk metadata"
    ))?;

    Ok(Some(metadata))
  }

  /// Reads a chunk body, returning it as a vector of numbers.
  /// Will return an error if the reader is not byte-aligned,
  /// the reader runs out of data, or any corruptions are found.
  ///
  /// Typically one would pass in the [`Flags`] obtained from an earlier
  /// [`.header()`][Self::header] call and the [`ChunkMetadata`] obtained
  /// from an earlier [`.chunk_metadata()`][Self::chunk_metadata] call.
  pub fn chunk_body(
    &self,
    reader: &mut BitReader,
    flags: &Flags,
    metadata: &ChunkMetadata<T>,
  ) -> QCompressResult<Vec<T>> {
    match &metadata.prefix_metadata {
      PrefixMetadata::Simple { prefixes } => {
        let chunk_decompressor = ChunkDecompressor::new(
          metadata.n,
          metadata.compressed_body_size,
          prefixes.clone(),
          self.config.clone(),
          flags.clone(),
        )?;
        chunk_decompressor.decompress_chunk_body(reader)
      },
      PrefixMetadata::Delta { delta_moments, prefixes } => {
        let n_deltas = max(
          metadata.n,
          delta_moments.order(),
        ) - delta_moments.order();
        let chunk_decompressor = ChunkDecompressor::new(
          n_deltas,
          metadata.compressed_body_size,
          prefixes.clone(),
          self.config.clone(),
          flags.clone(),
        )?;
        let deltas = chunk_decompressor.decompress_chunk_body(reader)?;
        let res = delta_encoding::reconstruct_nums(delta_moments, &deltas, metadata.n);
        Ok(res)
      }
    }
  }

  /// Reads a [`ChunkMetadata`] and the chunk body into a vector of numbers,
  /// returning both.
  /// Will return `None` if it instead finds a termination footer
  /// (indicating end of the .qco file).
  /// Will return an error if the reader is not byte-aligned,
  /// the reader runs out of data, or any corruptions are found.
  ///
  /// The same effect can be achieved via
  /// [`.chunk_metadata()`][Self::chunk_metadata] and
  /// [`.chunk_body()`][Self::chunk_body].
  pub fn chunk(
    &self,
    reader: &mut BitReader,
    flags: &Flags,
  ) -> QCompressResult<Option<DecompressedChunk<T>>> {
    let maybe_metadata = self.chunk_metadata(reader, flags)?;
    match maybe_metadata {
      Some(metadata) => {
        let nums = self.chunk_body(
          reader,
          flags,
          &metadata,
        )?;
        Ok(Some(DecompressedChunk {
          metadata,
          nums,
        }))
      },
      None => Ok(None)
    }
  }

  /// Takes in compressed bytes and returns a vector of numbers.
  /// Will return an error if there are any compatibility, corruption,
  /// or insufficient data issues.
  pub fn simple_decompress(&self, bytes: Vec<u8>) -> QCompressResult<Vec<T>> {
    // cloning/extending by a single chunk's numbers can slow down by 2%
    // so we just take ownership of the first chunk's numbers instead
    let mut reader = BitReader::from(bytes);
    let mut res: Option<Vec<T>> = None;
    let flags = self.header(&mut reader)?;
    while let Some(chunk) = self.chunk(&mut reader, &flags)? {
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
  use crate::{BitReader, Decompressor, Flags};
  use crate::chunk_metadata::{ChunkMetadata, PrefixMetadata};
  use crate::errors::{ErrorKind, QCompressResult};
  use crate::prefix::Prefix;

  fn prefix_w_code(code: Vec<bool>) -> Prefix<i64> {
    Prefix {
      count: 1,
      code,
      lower: 100,
      upper: 200,
      run_len_jumpstart: None,
    }
  }

  #[test]
  fn test_corrupt_prefixes_error_not_panic() -> QCompressResult<()> {
    let decompressor = Decompressor::<i64>::default();
    let bytes = vec![1, 2, 3, 4, 5, 6]; // not important for test

    let metadata_missing_prefix = ChunkMetadata::<i64> {
      n: 2,
      compressed_body_size: 1,
      prefix_metadata: PrefixMetadata::Simple { prefixes: vec![
        prefix_w_code(vec![false]),
        prefix_w_code(vec![true, false]),
      ]},
    };
    let metadata_duplicating_prefix = ChunkMetadata::<i64> {
      n: 2,
      compressed_body_size: 1,
      prefix_metadata: PrefixMetadata::Simple { prefixes: vec![
        prefix_w_code(vec![false]),
        prefix_w_code(vec![false]),
        prefix_w_code(vec![true]),
      ]}
    };

    let flags = Flags {
      use_5_bit_prefix_len: true,
      delta_encoding_order: 0,
    };

    for bad_metadata in vec![metadata_missing_prefix, metadata_duplicating_prefix] {
      let result = decompressor.chunk_body(
        &mut BitReader::from(bytes.clone()),
        &flags,
        &bad_metadata,
      );
      assert!(matches!(result.unwrap_err().kind, ErrorKind::Corruption));
    }

    Ok(())
  }
}

