use std::cmp::{max, min};
use std::fmt::Debug;
use std::marker::PhantomData;

use crate::{bits, Flags};
use crate::bit_reader::BitReader;
use crate::bit_words::BitWords;
use crate::chunk_metadata::{ChunkMetadata, DecompressedChunk, PrefixMetadata};
use crate::constants::*;
use crate::data_types::{NumberLike, UnsignedLike};
use crate::delta_encoding;
use crate::delta_encoding::DeltaMoments;
use crate::errors::{QCompressError, QCompressResult};
use crate::huffman_decoding::HuffmanTable;
use crate::prefix::{Prefix, PrefixDecompressionInfo};

const UNCHECKED_NUM_THRESHOLD: usize = 30;

/// All the settings you can specify about decompression.
#[derive(Clone, Debug, Default)]
pub struct DecompressorConfig {}

fn atomically<T, F>(reader: &mut BitReader, f: F) -> QCompressResult<T>
where F: FnOnce(&mut BitReader) -> QCompressResult<T> {
  let clean_bit_idx = reader.bit_idx();
  let res = f(reader);
  if res.is_err() {
    reader.seek_to(clean_bit_idx);
  }
  res
}

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

// For the prefix, the maximum number of bits we might need to read.
// Helps decide whether to do checked or unchecked reads.
fn max_bits_read<T: NumberLike>(p: &Prefix<T>) -> usize {
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

  prefix_bits + max_jumpstart_bits + max_reps * max_bits_per_offset
}

// For the prefix, the maximum number of bits we might overshoot by during an
// unchecked read.
// Helps decide whether to do checked or unchecked reads.
// We could make a slightly tighter bound with more logic, but I don't think there
// are any cases where it would help much.
fn max_bits_overshot<T: NumberLike>(p: &Prefix<T>) -> usize {
  if p.code.is_empty() {
    0
  } else {
    (MAX_PREFIX_TABLE_SIZE_LOG - 1).saturating_sub(p.k_info().k)
  }
}

#[derive(Clone, Copy, Debug)]
struct IncompletePrefix<Diff: UnsignedLike> {
  prefix: PrefixDecompressionInfo<Diff>,
  remaining_reps: usize,
}

#[derive(Clone, Debug)]
pub struct NumDecompressor<T> where T: NumberLike {
  // known information about the chunk
  huffman_table: HuffmanTable<T::Unsigned>,
  n: usize,
  compressed_body_size: usize,
  max_bits_per_num_block: usize,
  max_overshoot_per_num_block: usize,

  // mutable state
  nums_processed: usize,
  bits_processed: usize,
  incomplete_prefix: Option<IncompletePrefix<T::Unsigned>>,
}

impl<T> NumDecompressor<T> where T: NumberLike {
  pub(crate) fn new(
    n: usize,
    compressed_body_size: usize,
    prefixes: Vec<Prefix<T>>,
  ) -> QCompressResult<Self> {
    if prefixes.is_empty() && n > 0 {
      return Err(QCompressError::corruption(format!(
        "unable to decompress chunk with no prefixes and {} numbers",
        n,
      )));
    }
    validate_prefix_tree(&prefixes)?;

    let max_bits_per_num_block = prefixes.iter()
      .map(max_bits_read)
      .max()
      .unwrap_or(usize::MAX);
    let max_overshoot_per_num_block = prefixes.iter()
      .map(max_bits_overshot)
      .max()
      .unwrap_or(usize::MAX);

    Ok(NumDecompressor {
      huffman_table: HuffmanTable::from(&prefixes),
      n,
      compressed_body_size,
      max_bits_per_num_block,
      max_overshoot_per_num_block,
      nums_processed: 0,
      bits_processed: 0,
      incomplete_prefix: None,
    })
  }

  fn limit_reps(
    &mut self,
    prefix: PrefixDecompressionInfo<T::Unsigned>,
    full_reps: usize,
    limit: usize,
  ) -> usize {
    if full_reps > limit {
      self.incomplete_prefix = Some(IncompletePrefix {
        prefix,
        remaining_reps: full_reps - limit,
      });
      limit
    } else {
      full_reps
    }
  }

  fn unchecked_decompress_num_block(
    &mut self,
    reader: &mut BitReader,
    res: &mut Vec<T>,
    batch_size: usize,
  ) {
    let p = self.huffman_table.unchecked_search_with_reader(reader);

    match p.run_len_jumpstart {
      None => self.unchecked_decompress_offsets(reader, res, p, 1),
      // we stored the number of occurrences minus 1 because we knew it's at least 1
      Some(jumpstart) => {
        let full_reps = reader.unchecked_read_varint(jumpstart) + 1;
        let reps = self.limit_reps(p, full_reps, batch_size - res.len());
        self.unchecked_decompress_offsets(reader, res, p, reps);
      },
    };
  }

  fn unchecked_decompress_offsets(
    &self,
    reader: &mut BitReader,
    res: &mut Vec<T>,
    p: PrefixDecompressionInfo<T::Unsigned>,
    reps: usize,
  ) {
    if reps > 1 && p.k == 0 {
      // this branch is purely for performance reasons
      // the reps > 1 check also improves performance
      let num = T::from_unsigned(p.lower_unsigned);
      for _ in 0..reps {
        res.push(num);
      }
    } else {
      for _ in 0..reps {
        let mut offset = reader.unchecked_read_diff(p.k);
        if p.k < T::Unsigned::BITS &&
          p.k_range - offset >= p.most_significant &&
          reader.unchecked_read_one() {
          offset |= p.most_significant;
        }
        let num = T::from_unsigned(p.lower_unsigned + offset * p.gcd);
        res.push(num);
      }
    }
  }

  fn decompress_num_block(
    &mut self,
    reader: &mut BitReader,
    res: &mut Vec<T>,
    batch_size: usize,
  ) -> QCompressResult<()> {
    let p = self.huffman_table.search_with_reader(reader)?;

    let reps = match p.run_len_jumpstart {
      None => 1,
      // we stored the number of occurrences minus 1 because we knew it's at least 1
      Some(jumpstart) => {
        let full_reps = reader.read_varint(jumpstart)? + 1;
        self.limit_reps(p, full_reps, batch_size - res.len())
      },
    };
    self.decompress_offsets(reader, res, p, reps)
  }

  fn decompress_offsets(
    &self,
    reader: &mut BitReader,
    res: &mut Vec<T>,
    p: PrefixDecompressionInfo<T::Unsigned>,
    reps: usize,
  ) -> QCompressResult<()> {
    for _ in 0..reps {
      let mut offset = reader.read_diff(p.k)?;
      if p.k < T::Unsigned::BITS {
        let most_significant = T::Unsigned::ONE << p.k;
        if p.k_range - offset >= most_significant && reader.read_one()? {
          offset |= most_significant;
        }
      }
      let num = T::from_unsigned(p.lower_unsigned + offset * p.gcd);
      res.push(num);
    }

    Ok(())
  }

  // After much debugging a performance degradation from error handling changes,
  // it turned out this function's logic ran slower when inlining.
  // I don't understand why, but telling it not
  // to inline fixed the performance issue.
  // https://stackoverflow.com/questions/70911460/why-does-an-unrelated-heap-allocation-in-the-same-rust-scope-hurt-performance
  //
  // If this runs out of data, it returns an error and leaves reader unchanged.
  pub fn decompress_nums_limited(
    &mut self,
    reader: &mut BitReader,
    limit: usize,
  ) -> QCompressResult<Vec<T>> {
    atomically(reader, |r| {
      self.decompress_nums_limited_dirty(r, limit)
    })
  }

  #[inline(never)]
  fn decompress_nums_limited_dirty(
    &mut self,
    reader: &mut BitReader,
    limit: usize,
  ) -> QCompressResult<Vec<T>> {
    let batch_size = min(
      self.n - self.nums_processed,
      limit,
    );
    let mut res = Vec::with_capacity(batch_size);

    if batch_size == 0 {
      return Ok(res);
    }

    let start_bit_idx = reader.bit_idx();

    if let Some(IncompletePrefix { prefix, remaining_reps }) = self.incomplete_prefix {
      let reps = if remaining_reps <= batch_size {
        self.incomplete_prefix = None;
        remaining_reps
      } else {
        self.incomplete_prefix = Some(IncompletePrefix {
          prefix,
          remaining_reps: remaining_reps - batch_size,
        });
        batch_size
      };
      self.decompress_offsets(
        reader,
        &mut res,
        prefix,
        reps,
      )?;
    }

    if self.max_bits_per_num_block == 0 {
      let mut temp = Vec::with_capacity(1);
      self.unchecked_decompress_num_block(reader, &mut temp, 1);
      let constant_num = temp[0];
      while res.len() < batch_size {
        res.push(constant_num);
      }
    } else {
      loop {
        let remaining_nums = batch_size - res.len();
        let guaranteed_safe_num_blocks = min(
          remaining_nums,
          reader.bits_remaining().saturating_sub(self.max_overshoot_per_num_block) /
            self.max_bits_per_num_block,
        );

        if guaranteed_safe_num_blocks >= UNCHECKED_NUM_THRESHOLD {
          let mut block_idx = 0;
          while block_idx < guaranteed_safe_num_blocks && res.len() < self.n {
            self.unchecked_decompress_num_block(reader, &mut res, batch_size);
            block_idx += 1;
          }
        } else {
          break;
        }
      }

      while res.len() < batch_size {
        self.decompress_num_block(reader, &mut res, batch_size)?;
      }
    }

    self.nums_processed += batch_size;
    if self.nums_processed == self.n {
      reader.drain_empty_byte(|| QCompressError::corruption(
        "nonzero bits in end of final byte of chunk numbers"
      ))?;
    }
    let end_bit_idx = reader.bit_idx();
    self.bits_processed += end_bit_idx - start_bit_idx;

    if self.nums_processed == self.n {
      let compressed_body_bit_size = self.compressed_body_size * 8;
      if compressed_body_bit_size != self.bits_processed {
        return Err(QCompressError::corruption(format!(
          "expected the compressed body to contain {} bits but instead processed {}",
          compressed_body_bit_size,
          self.bits_processed,
        )));
      }
    }

    Ok(res)
  }
}

/// A low-level, stateful way to decompress small batches of numbers at a time.
///
/// After decompressing metadata with a `Decompressor`, you may call
/// [`Decompressor::get_chunk_body_decompressor`]
/// to create an instance of `ChunkBodyDecompressor`.
pub enum ChunkBodyDecompressor<T: NumberLike> {
  #[doc(hidden)]
  Simple {
    num_decompressor: NumDecompressor<T>,
  },
  #[doc(hidden)]
  Delta {
    n: usize,
    num_decompressor: NumDecompressor<T::Signed>,
    delta_moments: DeltaMoments<T>,
    nums_processed: usize,
  },
}

impl<T: NumberLike> ChunkBodyDecompressor<T> {
  pub(crate) fn new(metadata: &ChunkMetadata<T>) -> QCompressResult<Self> {
    Ok(match &metadata.prefix_metadata {
      PrefixMetadata::Simple { prefixes } => Self::Simple {
        num_decompressor: NumDecompressor::new(
          metadata.n,
          metadata.compressed_body_size,
          prefixes.clone()
        )?
      },
      PrefixMetadata::Delta { prefixes, delta_moments } => Self::Delta {
        n: metadata.n,
        num_decompressor: NumDecompressor::new(
          metadata.n.saturating_sub(delta_moments.order()),
          metadata.compressed_body_size,
          prefixes.clone()
        )?,
        delta_moments: delta_moments.clone(),
        nums_processed: 0,
      },
    })
  }

  /// Returns up to `limit` numbers from the `BitReader`.
  /// Will return an error if the reader runs out of data.
  ///
  /// This maintains an internal state allowing you to pick up where you left
  /// off. For example, calling on a chunk containing 11 numbers using limit 5
  /// repeatedly will return
  /// * the first 5 numbers in the first batch,
  /// * then the next 5,
  /// * then the last number,
  /// * then an empty vector for each following call.
  ///
  /// If this reaches an error, it leaves `reader` unchanged.
  pub fn decompress_next_batch(
    &mut self,
    reader: &mut BitReader,
    limit: usize,
  ) -> QCompressResult<Vec<T>> {
    match self {
      Self::Simple { num_decompressor } => num_decompressor.decompress_nums_limited(
        reader,
        limit
      ),
      Self::Delta {
        n,
        num_decompressor,
        delta_moments,
        nums_processed,
      } => {
        let batch_size = min(
          *n - *nums_processed,
          limit,
        );
        let deltas = num_decompressor.decompress_nums_limited(
          reader,
          limit,
        )?;
        let nums = delta_encoding::reconstruct_nums(
          delta_moments,
          &deltas,
          batch_size,
        );
        *nums_processed += batch_size;
        Ok(nums)
      }
    }
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
/// let nums = decompressor.simple_decompress(&my_bytes).expect("decompression"); // returns Vec<i32>
/// ```
/// You can also get full control over the decompression process:
/// ```
/// use q_compress::{BitReader, BitWords, Decompressor};
///
/// let my_bytes = vec![113, 99, 111, 33, 3, 0, 46]; // the simplest possible .qco file; empty
/// let my_words = BitWords::from(&my_bytes);
/// let mut reader = BitReader::from(&my_words);
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
  _config: DecompressorConfig,
  phantom: PhantomData<T>,
}

impl<T> Decompressor<T> where T: NumberLike {
  /// Creates a new decompressor, given a [`DecompressorConfig`].
  /// This config has nothing to do with [`Flags`], which will be parsed out
  /// of a .qco file's header.
  pub fn from_config(config: DecompressorConfig) -> Self {
    Self {
      _config: config,
      ..Default::default()
    }
  }

  /// Reads the header, returning its [`Flags`].
  /// Will return an error if the reader is not byte-aligned,
  /// if the reader runs out of data, if the data type byte does not agree,
  /// if the flags are from a newer, incompatible version of q_compress,
  /// or if any corruptions are found.
  ///
  /// If this reaches an error, it leaves `reader` unchanged.
  pub fn header(&self, reader: &mut BitReader) -> QCompressResult<Flags> {
    atomically(reader, |reader| {
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
    })
  }

  /// Reads a [`ChunkMetadata`], returning it.
  /// Will return `None` if it instead finds a termination footer
  /// (indicating end of the .qco file).
  /// Will return an error if the reader is not byte-aligned,
  /// the reader runs out of data, or any corruptions are found.
  ///
  /// Typically one would pass in the [`Flags`] obtained from an earlier
  /// [`.header()`][Self::header] call.
  ///
  /// If this reaches an error, it leaves `reader` unchanged.
  pub fn chunk_metadata(&self, reader: &mut BitReader, flags: &Flags) -> QCompressResult<Option<ChunkMetadata<T>>> {
    atomically(reader, |reader| {
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
    })
  }

  /// The lowest-level way to decompress numbers from a chunk.
  ///
  /// Returns a stateful `ChunkBodyDecompressor` that supports decompressing
  /// one batch of numbers at a time, rather than all numbers at once.
  /// Will return an error if the metadata is corrupt.
  /// This can be useful if you only need the first few numbers or you are
  /// working in memory-constrained conditions and cannot decompress a whole
  /// chunk at a time.
  ///
  /// ```
  /// use q_compress::{BitReader, BitWords, Decompressor};
  ///
  /// // .qco bytes for the boolean `true` repeated 2^24 - 1 times.
  /// // We'll read just the first 3.
  /// let my_bytes = vec![113, 99, 111, 33, 7, 0, 44, 255, 255, 255, 0, 0, 0, 0, 0, 3, 255, 255, 254, 2, 2, 0, 46];
  /// let my_words = BitWords::from(&my_bytes);
  /// let mut reader = BitReader::from(&my_words);
  /// let decompressor = Decompressor::<bool>::default();
  ///
  /// let flags = decompressor.header(&mut reader).unwrap();
  /// let metadata = decompressor.chunk_metadata(&mut reader, &flags).unwrap().unwrap();
  /// let mut chunk_body_decompressor = decompressor.get_chunk_body_decompressor(&flags, &metadata).unwrap();
  /// let head = chunk_body_decompressor.decompress_next_batch(&mut reader, 3).unwrap();
  /// assert_eq!(head, vec![true, true, true]);
  /// ```
  pub fn get_chunk_body_decompressor(
    &self,
    _flags: &Flags,
    metadata: &ChunkMetadata<T>,
  ) -> QCompressResult<ChunkBodyDecompressor<T>> {
    ChunkBodyDecompressor::new(metadata)
  }

  /// Reads a chunk body, returning it as a vector of numbers.
  /// Will return an error if the reader runs out of data
  /// or any corruptions are found.
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
    let mut chunk_body_decompressor = self.get_chunk_body_decompressor(
      flags,
      metadata,
    )?;
    chunk_body_decompressor.decompress_next_batch(
      reader,
      metadata.n,
    )
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
  /// If this reaches an error, it leaves `reader` unchanged.
  pub fn chunk(
    &self,
    reader: &mut BitReader,
    flags: &Flags,
  ) -> QCompressResult<Option<DecompressedChunk<T>>> {
    atomically(reader, |reader| {
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
    })
  }

  /// Takes in compressed bytes and returns a vector of numbers.
  /// Will return an error if there are any compatibility, corruption,
  /// or insufficient data issues.
  pub fn simple_decompress(&self, bytes: &[u8]) -> QCompressResult<Vec<T>> {
    // cloning/extending by a single chunk's numbers can slow down by 2%
    // so we just take ownership of the first chunk's numbers instead
    let words = BitWords::from(bytes);
    let mut reader = BitReader::from(&words);
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
  use crate::bit_words::BitWords;
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
      gcd: 1,
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
      use_5_bit_code_len: true,
      delta_encoding_order: 0,
      use_min_count_encoding: true,
      use_gcds: false,
    };

    for bad_metadata in vec![metadata_missing_prefix, metadata_duplicating_prefix] {
      let words = BitWords::from(&bytes);
      let mut reader = BitReader::from(&words);
      let result = decompressor.chunk_body(
        &mut reader,
        &flags,
        &bad_metadata,
      );
      assert!(matches!(result.unwrap_err().kind, ErrorKind::Corruption));
    }

    Ok(())
  }
}

