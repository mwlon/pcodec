use std::cmp::{max, min};

use crate::bin::BinDecompressionInfo;
use crate::bit_reader::BitReader;
use crate::constants::{Bitlen, BITS_TO_ENCODE_N_ENTRIES, MAX_BIN_TABLE_SIZE_LOG, MAX_ENTRIES};
use crate::data_types::{NumberLike, UnsignedLike};
use crate::errors::{ErrorKind, QCompressError, QCompressResult};
use crate::modes::gcd::{GcdMode};
use crate::huffman_decoding::HuffmanTable;
use crate::progress::Progress;
use crate::run_len_utils::{GeneralRunLenOp, RunLenOperator, TrivialRunLenOp};
use crate::{Bin, bits, run_len_utils};
use crate::modes::classic::ClassicMode;
use crate::modes::{DynMode, gcd};
use crate::modes::Mode;

const UNCHECKED_NUM_THRESHOLD: usize = 30;

fn validate_bin_tree<T: NumberLike>(bins: &[Bin<T>]) -> QCompressResult<()> {
  if bins.is_empty() {
    return Ok(());
  }

  let mut max_depth = 0;
  for bin in bins {
    max_depth = max(max_depth, bin.code_len);
  }

  let max_n_leafs = 1_usize << max_depth;
  let mut is_specifieds = vec![false; max_n_leafs];
  for bin in bins {
    let base_idx = bin.code;
    let step = 1_usize << bin.code_len;
    let n_leafs = 1_usize << (max_depth - bin.code_len);
    for is_specified in is_specifieds
      .iter_mut()
      .skip(base_idx)
      .step_by(step)
      .take(n_leafs)
    {
      if *is_specified {
        return Err(QCompressError::corruption(format!(
          "multiple bins for {} found in chunk metadata",
          bits::code_to_string(bin.code, bin.code_len),
        )));
      }
      *is_specified = true;
    }
  }
  for (idx, is_specified) in is_specifieds.iter().enumerate() {
    if !is_specified {
      return Err(QCompressError::corruption(format!(
        "no bins for {} found in chunk metadata",
        bits::code_to_string(idx, max_depth),
      )));
    }
  }
  Ok(())
}

// For the bin, the maximum number of bits we might need to read.
// Helps decide whether to do checked or unchecked reads.
fn max_bits_read<T: NumberLike>(bin: &Bin<T>) -> usize {
  let bin_bits = bin.code_len;
  let (max_reps, max_jumpstart_bits) = match bin.run_len_jumpstart {
    None => (1, 0),
    Some(_) => (MAX_ENTRIES, 2 * BITS_TO_ENCODE_N_ENTRIES),
  };
  let max_bits_per_offset = bin.offset_bits;
  bin_bits as usize + max_jumpstart_bits as usize + max_reps * max_bits_per_offset as usize
}

// For the bin, the maximum number of bits we might overshoot by during an
// unchecked read.
// Helps decide whether to do checked or unchecked reads.
// We could make a slightly tighter bound with more logic, but I don't think there
// are any cases where it would help much.
fn max_bits_overshot<T: NumberLike>(bin: &Bin<T>) -> Bitlen {
  if bin.code_len == 0 {
    0
  } else {
    (MAX_BIN_TABLE_SIZE_LOG - 1).saturating_sub(bin.offset_bits)
  }
}

#[derive(Clone, Debug)]
struct State<U: UnsignedLike> {
  n_processed: usize,
  bits_processed: usize,
  incomplete_bin: BinDecompressionInfo<U>,
  incomplete_reps: usize,
}

// NumDecompressor does the main work of decoding bytes into NumberLikes
#[derive(Clone, Debug)]
pub struct NumDecompressor<U: UnsignedLike> {
  // known information about the chunk
  huffman_table: HuffmanTable<U>,
  n: usize,
  compressed_body_size: usize,
  max_bits_per_num_block: usize,
  max_overshoot_per_num_block: Bitlen,
  use_run_len: bool,
  dyn_mode: DynMode,

  // mutable state
  state: State<U>,
}

impl<U: UnsignedLike> NumDecompressor<U> {
  pub(crate) fn new<T: NumberLike<Unsigned = U>>(
    n: usize,
    compressed_body_size: usize,
    bins: &[Bin<T>],
  ) -> QCompressResult<Self> {
    if bins.is_empty() && n > 0 {
      return Err(QCompressError::corruption(format!(
        "unable to decompress chunk with no bins and {} numbers",
        n,
      )));
    }
    validate_bin_tree(bins)?;

    let max_bits_per_num_block = bins.iter().map(max_bits_read).max().unwrap_or(usize::MAX);
    let max_overshoot_per_num_block = bins
      .iter()
      .map(max_bits_overshot)
      .max()
      .unwrap_or(Bitlen::MAX);
    let use_run_len = run_len_utils::use_run_len(&bins);
    let dyn_mode = if gcd::use_gcd_arithmetic(bins) {
      DynMode::Gcd
    } else {
      DynMode::Classic
    };

    Ok(NumDecompressor {
      huffman_table: HuffmanTable::from(bins),
      n,
      compressed_body_size,
      max_bits_per_num_block,
      max_overshoot_per_num_block,
      use_run_len,
      dyn_mode,
      state: State {
        n_processed: 0,
        bits_processed: 0,
        incomplete_bin: BinDecompressionInfo::default(),
        incomplete_reps: 0,
      },
    })
  }

  pub fn bits_remaining(&self) -> usize {
    self.compressed_body_size * 8 - self.state.bits_processed
  }

  #[inline]
  fn unchecked_decompress_num_block<M: Mode<U>, RunLenOp: RunLenOperator>(
    &mut self,
    reader: &mut BitReader,
    mode: M,
    dest: &mut [U],
  ) -> usize {
    let bin = self.huffman_table.unchecked_search_with_reader(reader);
    RunLenOp::unchecked_decompress_for_bin::<U, M>(self, reader, bin, mode, dest)
  }

  // returns count of numbers processed
  #[inline(never)]
  fn unchecked_decompress_num_blocks<M: Mode<U>, RunLenOp: RunLenOperator>(
    &mut self,
    reader: &mut BitReader,
    mut guaranteed_safe_num_blocks: usize,
    mode: M,
    batch_size: usize,
    n_processed: &mut usize,
    dest: &mut [U],
  ) {
    while guaranteed_safe_num_blocks > 0 && RunLenOp::batch_ongoing(*n_processed, batch_size) {
      *n_processed += self.unchecked_decompress_num_block::<M, RunLenOp>(
        reader,
        mode,
        &mut dest[*n_processed..batch_size],
      );
      guaranteed_safe_num_blocks -= 1;
    }
  }

  pub fn unchecked_limit_reps(
    &mut self,
    bin: BinDecompressionInfo<U>,
    full_reps: usize,
    limit: usize,
  ) -> usize {
    if full_reps > limit {
      self.state.incomplete_bin = bin;
      self.state.incomplete_reps = full_reps - limit;
      limit
    } else {
      full_reps
    }
  }

  fn decompress_num_block<M: Mode<U>>(
    &mut self,
    reader: &mut BitReader,
    mode: M,
    dest: &mut [U],
  ) -> QCompressResult<usize> {
    let start_bit_idx = reader.bit_idx();
    let bin_res = self.huffman_table.search_with_reader(reader);
    if bin_res.is_err() {
      reader.seek_to(start_bit_idx);
    }
    let bin = bin_res?;

    match bin.run_len_jumpstart {
      None => {
        let res = mode.decompress_unsigned(bin, reader);
        if res.is_err() {
          reader.seek_to(start_bit_idx);
        }
        dest[0] = res?;
        Ok(1)
      }
      // we stored the number of occurrences minus 1 because we knew it's at least 1
      Some(jumpstart) => {
        let full_reps_minus_one_res = reader.read_varint(jumpstart);
        if full_reps_minus_one_res.is_err() {
          reader.seek_to(start_bit_idx);
        }
        let full_reps = full_reps_minus_one_res? + 1;
        self.state.incomplete_bin = bin;
        self.state.incomplete_reps = full_reps;
        let reps = min(full_reps, dest.len());
        self.decompress_offsets(reader, bin, mode, reps, dest)?;
        self.state.incomplete_reps -= reps;
        Ok(reps)
      }
    }
  }

  // errors on insufficient data, but updates unsigneds with last complete number
  // and leaves reader at end end of last complete number
  fn decompress_offsets<M: Mode<U>>(
    &self,
    reader: &mut BitReader,
    bin: BinDecompressionInfo<U>,
    mode: M,
    reps: usize,
    dest: &mut [U],
  ) -> QCompressResult<()> {
    for i in 0..reps {
      let start_bit_idx = reader.bit_idx();
      let u = mode.decompress_unsigned(bin, reader);
      if u.is_err() {
        reader.seek_to(start_bit_idx);
      }
      dest[i] = u?;
    }

    Ok(())
  }

  // If hits a corruption, it returns an error and leaves reader and self unchanged.
  // State managed here: n_processed, bits_processed
  fn decompress_unsigneds_with_mode<M: Mode<U>>(
    &mut self,
    reader: &mut BitReader,
    error_on_insufficient_data: bool,
    mode: M,
    dest: &mut [U],
  ) -> QCompressResult<Progress> {
    let initial_reader = reader.clone();
    let initial_state = self.state.clone();
    let res = self.decompress_unsigneds_dirty(reader, error_on_insufficient_data, mode, dest);
    match &res {
      Ok(progress) => {
        self.state.n_processed += progress.n_processed;

        if progress.finished_body {
          reader.drain_empty_byte("nonzero bits in end of final byte of chunk numbers")?;
        }
        self.state.bits_processed += reader.bit_idx() - initial_reader.bit_idx();
        if progress.finished_body {
          let compressed_body_bit_size = self.compressed_body_size * 8;
          if compressed_body_bit_size != self.state.bits_processed {
            return Err(QCompressError::corruption(format!(
              "expected the compressed body to contain {} bits but instead processed {}",
              compressed_body_bit_size, self.state.bits_processed,
            )));
          }
        }
      }
      Err(_) => {
        *reader = initial_reader;
        self.state = initial_state;
      }
    }
    res
  }

  // After much debugging a performance degradation from error handling changes,
  // it turned out this function's logic ran slower when inlining.
  // I don't understand why, but telling it not
  // to inline fixed the performance issue.
  // https://stackoverflow.com/questions/70911460/why-does-an-unrelated-heap-allocation-in-the-same-rust-scope-hurt-performance
  //
  // state managed here: incomplete_bin
  #[inline(never)]
  fn decompress_unsigneds_dirty<M: Mode<U>>(
    &mut self,
    reader: &mut BitReader,
    error_on_insufficient_data: bool,
    mode: M,
    dest: &mut [U],
  ) -> QCompressResult<Progress> {
    let batch_size = min(self.n - self.state.n_processed, dest.len());
    // we'll modify this result if we encounter an insufficient data error
    let mut res = Progress {
      finished_body: batch_size >= self.n - self.state.n_processed,
      ..Default::default()
    };

    // treating this case (constant data) as special improves its performance
    if self.max_bits_per_num_block == 0 {
      let constant_num = self
        .huffman_table
        .unchecked_search_with_reader(reader)
        .lower_unsigned;
      dest[0..batch_size].fill(constant_num);
      res.n_processed = batch_size;
      return Ok(res);
    }

    if batch_size == 0 {
      return Ok(res);
    }

    let mark_insufficient = |mut progress: Progress, e: QCompressError| {
      if error_on_insufficient_data {
        Err(e)
      } else {
        progress.finished_body = false;
        progress.insufficient_data = true;
        Ok(progress)
      }
    };

    let incomplete_reps = self.state.incomplete_reps;
    if incomplete_reps > 0 {
      let reps = min(incomplete_reps, batch_size);
      let incomplete_res = self.decompress_offsets(reader, self.state.incomplete_bin, mode, reps, dest);
      self.state.incomplete_reps -= reps;
      res.n_processed += reps;
      match incomplete_res {
        Ok(_) => (),
        Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => {
          return mark_insufficient(res, e)
        }
        Err(e) => return Err(e),
      };
    }

    // as long as there's enough compressed data available, we don't need checked operations
    loop {
      let remaining_unsigneds = batch_size - res.n_processed;
      let guaranteed_safe_num_blocks = min(
        remaining_unsigneds,
        reader
          .bits_remaining()
          .saturating_sub(self.max_overshoot_per_num_block as usize)
          / self.max_bits_per_num_block,
      );

      if guaranteed_safe_num_blocks >= UNCHECKED_NUM_THRESHOLD {
        // don't slow down the tight loops with runtime checks - do these upfront to choose
        // the best compiled tight loop
        if self.use_run_len {
          self.unchecked_decompress_num_blocks::<M, GeneralRunLenOp>(
            reader,
            guaranteed_safe_num_blocks,
            mode,
            batch_size,
            &mut res.n_processed,
            dest,
          )
        } else {
          self.unchecked_decompress_num_blocks::<M, TrivialRunLenOp>(
            reader,
            guaranteed_safe_num_blocks,
            mode,
            batch_size,
            &mut res.n_processed,
            dest,
          )
        }
      } else {
        break;
      }
    }

    // do checked operations for the rest
    while res.n_processed < batch_size {
      res.n_processed += match self.decompress_num_block(reader, mode, &mut dest[res.n_processed..]) {
        Ok(n_processed) => n_processed,
        Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => {
          return mark_insufficient(res, e)
        }
        Err(e) => return Err(e),
      };
    }

    Ok(res)
  }

  pub fn decompress_unsigneds(
    &mut self,
    reader: &mut BitReader,
    error_on_insufficient_data: bool,
    dest: &mut [U],
  ) -> QCompressResult<Progress> {
    match self.dyn_mode {
      DynMode::Gcd => self.decompress_unsigneds_with_mode(reader, error_on_insufficient_data, GcdMode, dest),
      DynMode::Classic => self.decompress_unsigneds_with_mode(reader, error_on_insufficient_data, ClassicMode, dest),
    }
  }
}
