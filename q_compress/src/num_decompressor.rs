use std::cmp::{max, min};

use crate::bit_reader::BitReader;
use crate::{bits, gcd_utils, Prefix, run_len_utils};
use crate::constants::{BITS_TO_ENCODE_N_ENTRIES, MAX_ENTRIES, MAX_PREFIX_TABLE_SIZE_LOG};
use crate::data_types::{NumberLike, UnsignedLike};
use crate::errors::{ErrorKind, QCompressError, QCompressResult};
use crate::gcd_utils::{GcdOperator, GeneralGcdOp, TrivialGcdOp};
use crate::huffman_decoding::HuffmanTable;
use crate::prefix::PrefixDecompressionInfo;
use crate::run_len_utils::{GeneralRunLenOp, RunLenOperator, TrivialRunLenOp};

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

pub struct Unsigneds<U: UnsignedLike> {
  pub unsigneds: Vec<U>,
  pub finished_body: bool,
}

#[derive(Clone, Debug)]
struct State<U: UnsignedLike> {
  n_processed: usize,
  bits_processed: usize,
  incomplete_prefix: PrefixDecompressionInfo<U>,
  incomplete_reps: usize,
}

// NumDecompressor does the main work of decoding bytes into NumberLikes
#[derive(Clone, Debug)]
pub struct NumDecompressor<U> where U: UnsignedLike {
  // known information about the chunk
  huffman_table: HuffmanTable<U>,
  n: usize,
  compressed_body_size: usize,
  max_bits_per_num_block: usize,
  max_overshoot_per_num_block: usize,
  use_gcd: bool,
  use_run_len: bool,

  // mutable state
  state: State<U>,
}

// errors on insufficient data
fn decompress_offset_dirty<U: UnsignedLike>(
  reader: &mut BitReader,
  unsigneds: &mut Vec<U>,
  p: PrefixDecompressionInfo<U>,
) -> QCompressResult<()> {
  let mut offset = reader.read_diff::<U>(p.k)?;
  if offset < p.min_unambiguous_k_bit_offset && reader.read_one()? {
    offset |= p.most_significant;
  }
  let unsigned = p.lower_unsigned + offset * p.gcd;
  unsigneds.push(unsigned);
  Ok(())
}

impl<U> NumDecompressor<U> where U: UnsignedLike {
  pub(crate) fn new<T: NumberLike<Unsigned=U>>(
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
    let use_run_len = run_len_utils::use_run_len(&prefixes);

    Ok(NumDecompressor {
      huffman_table: HuffmanTable::from(&prefixes),
      n,
      compressed_body_size,
      max_bits_per_num_block,
      max_overshoot_per_num_block,
      use_gcd,
      use_run_len,
      state: State {
        n_processed: 0,
        bits_processed: 0,
        incomplete_prefix: PrefixDecompressionInfo::default(),
        incomplete_reps: 0,
      },
    })
  }

  pub fn bits_remaining(&self) -> usize {
    self.compressed_body_size * 8 - self.state.bits_processed
  }

  #[inline]
  fn unchecked_decompress_num_block<GcdOp: GcdOperator<U>, RunLenOp: RunLenOperator>(
    &mut self,
    reader: &mut BitReader,
    unsigneds: &mut Vec<U>,
    batch_size: usize,
  ) {
    let p = self.huffman_table.unchecked_search_with_reader(reader);
    RunLenOp::unchecked_decompress_offsets::<U, GcdOp>(self, reader, unsigneds, p, batch_size);
  }

  fn unchecked_decompress_num_blocks<GcdOp: GcdOperator<U>, RunLenOp: RunLenOperator>(
    &mut self,
    reader: &mut BitReader,
    unsigneds: &mut Vec<U>,
    guaranteed_safe_num_blocks: usize,
    batch_size: usize,
  ) {
    let mut block_idx = 0;
    while block_idx < guaranteed_safe_num_blocks && unsigneds.len() < self.n {
      self.unchecked_decompress_num_block::<GcdOp, RunLenOp>(reader, unsigneds, batch_size);
      block_idx += 1;
    }
  }

  pub fn unchecked_limit_reps(
    &mut self,
    prefix: PrefixDecompressionInfo<U>,
    full_reps: usize,
    limit: usize,
  ) -> usize {
    if full_reps > limit {
      self.state.incomplete_prefix = prefix;
      self.state.incomplete_reps = full_reps - limit;
      limit
    } else {
      full_reps
    }
  }

  pub fn limit_reps(
    &mut self,
    prefix: PrefixDecompressionInfo<U>,
    full_reps: usize,
    limit: usize,
  ) -> usize {
    self.state.incomplete_prefix = prefix;
    self.state.incomplete_reps = full_reps;
    min(full_reps, limit)
  }

  fn decompress_num_block(
    &mut self,
    reader: &mut BitReader,
    unsigneds: &mut Vec<U>,
    batch_size: usize,
  ) -> QCompressResult<()> {
    let start_bit_idx = reader.bit_idx();
    let pref_res = self.huffman_table.search_with_reader(reader);
    if pref_res.is_err() {
      reader.seek_to(start_bit_idx);
    }
    let prefix = pref_res?;

    match prefix.run_len_jumpstart {
      None => {
        let res = decompress_offset_dirty(reader, unsigneds, prefix);
        if res.is_err() {
          reader.seek_to(start_bit_idx);
        }
        res
      },
      // we stored the number of occurrences minus 1 because we knew it's at least 1
      Some(jumpstart) => {
        let full_reps_minus_one_res = reader.read_varint(jumpstart);
        if full_reps_minus_one_res.is_err() {
          reader.seek_to(start_bit_idx);
        }
        let full_reps = full_reps_minus_one_res? + 1;
        let reps = self.limit_reps(prefix, full_reps, batch_size - unsigneds.len());
        let start_count = unsigneds.len();
        let res = self.decompress_offsets(reader, unsigneds, prefix, reps);
        let n_processed = unsigneds.len() - start_count;
        self.state.incomplete_reps -= n_processed;
        res
      },
    }
  }

  // errors on insufficient data, but updates unsigneds with last complete number
  // and leaves reader at end end of last complete number
  fn decompress_offsets(
    &self,
    reader: &mut BitReader,
    unsigneds: &mut Vec<U>,
    p: PrefixDecompressionInfo<U>,
    reps: usize,
  ) -> QCompressResult<()> {
    for _ in 0..reps {
      let start_bit_idx = reader.bit_idx();
      let maybe_err = decompress_offset_dirty(reader, unsigneds, p);
      if maybe_err.is_err() {
        reader.seek_to(start_bit_idx);
        return maybe_err;
      }
    }

    Ok(())
  }

  // If hits a corruption, it returns an error and leaves reader and self unchanged.
  // State managed here: n_processed, bits_processed
  pub fn decompress_unsigneds_limited(
    &mut self,
    reader: &mut BitReader,
    limit: usize,
    error_on_insufficient_data: bool,
  ) -> QCompressResult<Unsigneds<U>> {
    let initial_reader = reader.clone();
    let initial_state = self.state.clone();
    let res = self.decompress_unsigneds_limited_dirty(reader, limit, error_on_insufficient_data);
    match &res {
      Ok(numbers) => {
        self.state.n_processed += numbers.unsigneds.len();

        if numbers.finished_body {
          reader.drain_empty_byte("nonzero bits in end of final byte of chunk numbers")?;
        }
        self.state.bits_processed += reader.bit_idx() - initial_reader.bit_idx();
        if numbers.finished_body {
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
  fn decompress_unsigneds_limited_dirty(
    &mut self,
    reader: &mut BitReader,
    limit: usize,
    error_on_insufficient_data: bool,
  ) -> QCompressResult<Unsigneds<U>> {
    let batch_size = min(
      self.n - self.state.n_processed,
      limit,
    );
    // we'll modify this result as we decode numbers and if we encounter an insufficient data error
    let finished_body = limit >= self.n - self.state.n_processed;
    let mut res = Unsigneds {
      unsigneds: Vec::with_capacity(batch_size),
      finished_body,
    };
    let unsigneds = &mut res.unsigneds;

    // treating this case (constant data) as special improves its performance
    if self.max_bits_per_num_block == 0 {
      let constant_num = self.huffman_table.unchecked_search_with_reader(reader).lower_unsigned;
      unsigneds.resize(batch_size, constant_num);
      return Ok(res);
    }

    if batch_size == 0 {
      return Ok(res);
    }

    let mark_insufficient = |mut numbers: Unsigneds<U>, e: QCompressError| {
      if error_on_insufficient_data {
        Err(e)
      } else {
        numbers.finished_body = false;
        Ok(numbers)
      }
    };

    let incomplete_reps = self.state.incomplete_reps;
    if incomplete_reps > 0 {
      let reps = min(incomplete_reps, batch_size);
      let incomplete_res = self.decompress_offsets(
        reader,
        unsigneds,
        self.state.incomplete_prefix,
        reps,
      );
      self.state.incomplete_reps -= unsigneds.len();
      match incomplete_res {
        Ok(_) => (),
        Err(e) if matches!(e.kind, ErrorKind::InsufficientData) =>
          return mark_insufficient(res, e),
        Err(e) => return Err(e),
      };
    }

    // as long as there's enough compressed data available, we don't need checked operations
    loop {
      let remaining_unsigneds = batch_size - unsigneds.len();
      let guaranteed_safe_num_blocks = min(
        remaining_unsigneds,
        reader.bits_remaining().saturating_sub(self.max_overshoot_per_num_block) /
          self.max_bits_per_num_block,
      );

      if guaranteed_safe_num_blocks >= UNCHECKED_NUM_THRESHOLD {
        // don't slow down the tight loops with runtime checks - do these upfront to choose
        // the best compiled tight loop
        match (self.use_gcd, self.use_run_len) {
          (false, false) => self.unchecked_decompress_num_blocks::<TrivialGcdOp, TrivialRunLenOp>(reader, unsigneds, guaranteed_safe_num_blocks, batch_size),
          (false, true) => self.unchecked_decompress_num_blocks::<TrivialGcdOp, GeneralRunLenOp>(reader, unsigneds, guaranteed_safe_num_blocks, batch_size),
          (true, false) => self.unchecked_decompress_num_blocks::<GeneralGcdOp, TrivialRunLenOp>(reader, unsigneds, guaranteed_safe_num_blocks, batch_size),
          (true, true) => self.unchecked_decompress_num_blocks::<GeneralGcdOp, GeneralRunLenOp>(reader, unsigneds, guaranteed_safe_num_blocks, batch_size),
        }
      } else {
        break;
      }
    }

    // do checked operations for the rest
    while unsigneds.len() < batch_size {
      match self.decompress_num_block(reader, unsigneds, batch_size) {
        Ok(_) => (),
        Err(e) if matches!(e.kind, ErrorKind::InsufficientData) =>
          return mark_insufficient(res, e),
        Err(e) => return Err(e),
      }
    }

    Ok(res)
  }
}
