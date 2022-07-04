use std::cmp::{max, min};

use crate::bit_reader::BitReader;
use crate::{bits, gcd_utils, Prefix};
use crate::constants::{BITS_TO_ENCODE_N_ENTRIES, MAX_ENTRIES, MAX_PREFIX_TABLE_SIZE_LOG};
use crate::data_types::{NumberLike, UnsignedLike};
use crate::errors::{ErrorKind, QCompressError, QCompressResult};
use crate::gcd_utils::{GcdOperator, GeneralGcdOp, TrivialGcdOp};
use crate::huffman_decoding::HuffmanTable;
use crate::prefix::PrefixDecompressionInfo;

const UNCHECKED_NUM_THRESHOLD: usize = 30;

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

pub struct Numbers<T: NumberLike> {
  pub nums: Vec<T>,
  // pub insufficient_data_err: Option<QCompressError>,
  pub finished_chunk_body: bool,
}

#[derive(Clone, Copy, Debug)]
struct IncompletePrefix<U: UnsignedLike> {
  prefix: PrefixDecompressionInfo<U>,
  remaining_reps: usize,
}

#[derive(Clone, Debug)]
struct State<U: UnsignedLike> {
  nums_processed: usize,
  bits_processed: usize,
  incomplete_prefix: Option<IncompletePrefix<U>>,
}

// NumDecompressor does the main work of decoding bytes into NumberLikes
#[derive(Clone, Debug)]
pub struct NumDecompressor<T> where T: NumberLike {
  // known information about the chunk
  huffman_table: HuffmanTable<T::Unsigned>,
  n: usize,
  compressed_body_size: usize,
  max_bits_per_num_block: usize,
  max_overshoot_per_num_block: usize,
  use_gcd: bool,

  // mutable state
  state: State<T::Unsigned>,
}

#[inline(always)]
fn unchecked_decompress_offsets<T: NumberLike, GcdOp: GcdOperator<T::Unsigned>>(
  reader: &mut BitReader,
  nums: &mut Vec<T>,
  p: PrefixDecompressionInfo<T::Unsigned>,
  reps: usize,
) {
  if reps > 1 && p.k == 0 {
    // this branch is purely for performance reasons
    // the reps > 1 check also improves performance
    let num = T::from_unsigned(p.lower_unsigned);
    for _ in 0..reps {
      nums.push(num);
    }
  } else {
    for _ in 0..reps {
      let mut offset = reader.unchecked_read_diff(p.k);
      if p.k < T::Unsigned::BITS &&
        p.k_range - offset >= p.most_significant &&
        reader.unchecked_read_one() {
        offset |= p.most_significant;
      }
      let num = T::from_unsigned(p.lower_unsigned + GcdOp::get_diff(offset, p.gcd));
      nums.push(num);
    }
  }
}

// errors on insufficient data
fn decompress_offset_dirty<T: NumberLike>(
  reader: &mut BitReader,
  nums: &mut Vec<T>,
  p: PrefixDecompressionInfo<T::Unsigned>,
) -> QCompressResult<()> {
  let mut offset = reader.read_diff(p.k)?;
  if p.k < T::Unsigned::BITS {
    let most_significant = T::Unsigned::ONE << p.k;
    if p.k_range - offset >= most_significant && reader.read_one()? {
      offset |= most_significant;
    }
  }
  let num = T::from_unsigned(p.lower_unsigned + offset * p.gcd);
  nums.push(num);
  Ok(())
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
    let use_gcd = gcd_utils::use_gcd_arithmetic(&prefixes);

    Ok(NumDecompressor {
      huffman_table: HuffmanTable::from(&prefixes),
      n,
      compressed_body_size,
      max_bits_per_num_block,
      max_overshoot_per_num_block,
      use_gcd,
      state: State {
        nums_processed: 0,
        bits_processed: 0,
        incomplete_prefix: None,
      },
    })
  }

  pub fn bits_remaining(&self) -> usize {
    self.compressed_body_size * 8 - self.state.bits_processed
  }

  fn limit_reps(
    &mut self,
    prefix: PrefixDecompressionInfo<T::Unsigned>,
    full_reps: usize,
    limit: usize,
  ) -> usize {
    if full_reps > limit {
      self.state.incomplete_prefix = Some(IncompletePrefix {
        prefix,
        remaining_reps: full_reps - limit,
      });
      limit
    } else {
      full_reps
    }
  }

  #[inline(always)]
  fn unchecked_decompress_num_block<GcdOp: GcdOperator<T::Unsigned>>(
    &mut self,
    reader: &mut BitReader,
    nums: &mut Vec<T>,
    batch_size: usize,
  ) {
    let p = self.huffman_table.unchecked_search_with_reader(reader);

    match p.run_len_jumpstart {
      None => unchecked_decompress_offsets::<T, GcdOp>(reader, nums, p, 1),
      // we stored the number of occurrences minus 1 because we knew it's at least 1
      Some(jumpstart) => {
        let full_reps = reader.unchecked_read_varint(jumpstart) + 1;
        let reps = self.limit_reps(p, full_reps, batch_size - nums.len());
        unchecked_decompress_offsets::<T, GcdOp>(reader, nums, p, reps);
      },
    };
  }

  fn decompress_num_block(
    &mut self,
    reader: &mut BitReader,
    nums: &mut Vec<T>,
    batch_size: usize,
  ) -> QCompressResult<()> {
    let p = self.huffman_table.search_with_reader(reader)?;

    let reps = match p.run_len_jumpstart {
      None => 1,
      // we stored the number of occurrences minus 1 because we knew it's at least 1
      Some(jumpstart) => {
        let full_reps = reader.read_varint(jumpstart)? + 1;
        self.limit_reps(p, full_reps, batch_size - nums.len())
      },
    };
    self.decompress_offsets(reader, nums, p, reps)
  }

  // errors on insufficient data, but updates nums with last complete number
  // and leaves reader at end end of last complete number
  fn decompress_offsets(
    &self,
    reader: &mut BitReader,
    nums: &mut Vec<T>,
    p: PrefixDecompressionInfo<T::Unsigned>,
    reps: usize,
  ) -> QCompressResult<()> {
    for _ in 0..reps {
      let start_bit_idx = reader.bit_idx();
      let maybe_err = decompress_offset_dirty(reader, nums, p);
      if maybe_err.is_err() {
        reader.seek_to(start_bit_idx);
        return maybe_err;
      }
    }

    Ok(())
  }

  // If hits a corruption, it returns an error and leaves reader and self unchanged.
  // State managed here: nums_processed, bits_processed
  pub fn decompress_nums_limited(
    &mut self,
    reader: &mut BitReader,
    limit: usize,
    error_on_insufficient_data: bool,
  ) -> QCompressResult<Numbers<T>> {
    let initial_reader = reader.clone();
    let initial_state = self.state.clone();
    let res = if self.use_gcd {
      self.decompress_nums_limited_dirty::<GeneralGcdOp>(reader, limit, error_on_insufficient_data)
    } else {
      self.decompress_nums_limited_dirty::<TrivialGcdOp>(reader, limit, error_on_insufficient_data)
    };
    match &res {
      Ok(numbers) => {
        self.state.nums_processed += numbers.nums.len();

        if numbers.finished_chunk_body {
          reader.drain_empty_byte(|| QCompressError::corruption(
            "nonzero bits in end of final byte of chunk numbers"
          ))?;
        }
        self.state.bits_processed += reader.bit_idx() - initial_reader.bit_idx();
        if numbers.finished_chunk_body {
          let compressed_body_bit_size = self.compressed_body_size * 8;
          if compressed_body_bit_size != self.state.bits_processed {
            return Err(QCompressError::corruption(format!(
              "expected the compressed body to contain {} bits but instead processed {}",
              compressed_body_bit_size,
              self.state.bits_processed,
            )));
          }
        }
      },
      Err(_) => {
        *reader = initial_reader;
        self.state = initial_state;
      },
    }
    res
  }

  // After much debugging a performance degradation from error handling changes,
  // it turned out this function's logic ran slower when inlining.
  // I don't understand why, but telling it not
  // to inline fixed the performance issue.
  // https://stackoverflow.com/questions/70911460/why-does-an-unrelated-heap-allocation-in-the-same-rust-scope-hurt-performance
  //
  // state managed here: incomplete_prefix
  #[inline(never)]
  fn decompress_nums_limited_dirty<GcdOp: GcdOperator<T::Unsigned>>(
    &mut self,
    reader: &mut BitReader,
    limit: usize,
    error_on_insufficient_data: bool,
  ) -> QCompressResult<Numbers<T>> {
    let batch_size = min(
      self.n - self.state.nums_processed,
      limit,
    );
    // we'll modify this result as we decode numbers and if we encounter an insufficient data error
    let completed_body = limit >= self.n - self.state.nums_processed;
    let mut numbers = Numbers {
      nums: Vec::with_capacity(batch_size),
      finished_chunk_body: completed_body,
    };
    let nums = &mut numbers.nums;

    if batch_size == 0 {
      return Ok(numbers);
    }

    let mark_insufficient = |mut numbers: Numbers<T>, e: QCompressError| {
      if error_on_insufficient_data {
        Err(e)
      } else {
        numbers.finished_chunk_body = false;
        Ok(numbers)
      }
    };

    if let Some(IncompletePrefix {
      prefix,
      remaining_reps
    }) = self.state.incomplete_prefix {
      let reps = min(remaining_reps, batch_size);
      let incomplete_res = self.decompress_offsets(
        reader,
        nums,
        prefix,
        reps,
      );
      let remaining_reps = remaining_reps - nums.len();
      if remaining_reps == 0 {
        self.state.incomplete_prefix = None;
      } else {
        self.state.incomplete_prefix.as_mut().unwrap().remaining_reps = remaining_reps;
      }
      match incomplete_res {
        Ok(_) => (),
        Err(e) if matches!(e.kind, ErrorKind::InsufficientData) =>
          return mark_insufficient(numbers, e),
        Err(e) => return Err(e),
      };
    }

    if self.max_bits_per_num_block == 0 {
      let mut temp = Vec::with_capacity(1);
      self.unchecked_decompress_num_block::<GcdOp>(reader, &mut temp, 1);
      let constant_num = temp[0];
      while nums.len() < batch_size {
        nums.push(constant_num);
      }
    } else {
      loop {
        let remaining_nums = batch_size - nums.len();
        let guaranteed_safe_num_blocks = min(
          remaining_nums,
          reader.bits_remaining().saturating_sub(self.max_overshoot_per_num_block) /
            self.max_bits_per_num_block,
        );

        if guaranteed_safe_num_blocks >= UNCHECKED_NUM_THRESHOLD {
          let mut block_idx = 0;
          while block_idx < guaranteed_safe_num_blocks && nums.len() < self.n {
            self.unchecked_decompress_num_block::<GcdOp>(reader, nums, batch_size);
            block_idx += 1;
          }
        } else {
          break;
        }
      }

      while nums.len() < batch_size {
        match self.decompress_num_block(reader, nums, batch_size) {
          Ok(_) => (),
          Err(e) if matches!(e.kind, ErrorKind::InsufficientData) =>
            return mark_insufficient(numbers, e),
          Err(e) => return Err(e),
        }
      }
    }

    Ok(numbers)
  }
}
