use std::cmp::{max, min};
use std::fmt::Debug;

use crate::{bits, run_len_utils, Bin, ChunkMetadata};

use crate::bit_reader::BitReader;
use crate::chunk_metadata::DataPageMetadata;
use crate::constants::{Bitlen, BITS_TO_ENCODE_N_ENTRIES, MAX_BIN_TABLE_SIZE_LOG, MAX_ENTRIES};
use crate::data_types::UnsignedLike;
use crate::errors::{ErrorKind, QCompressError, QCompressResult};
use crate::huffman_decoding::HuffmanTable;
use crate::modes::classic::ClassicMode;
use crate::modes::{DynMode, gcd};
use crate::modes::{Mode, ModeBin};
use crate::modes::float_mult::FloatMultMode;

use crate::modes::gcd::GcdMode;
use crate::progress::Progress;
use crate::run_len_utils::{GeneralRunLenOp, RunLenOperator, TrivialRunLenOp};

const UNCHECKED_NUM_THRESHOLD: usize = 30;

fn validate_bin_tree<U: UnsignedLike>(bins: &[Bin<U>]) -> QCompressResult<()> {
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
fn max_bits_read<U: UnsignedLike>(bin: &Bin<U>) -> usize {
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
fn max_bits_overshot<U: UnsignedLike>(bin: &Bin<U>) -> Bitlen {
  if bin.code_len == 0 {
    0
  } else {
    (MAX_BIN_TABLE_SIZE_LOG - 1).saturating_sub(bin.offset_bits)
  }
}

#[derive(Clone, Debug, Default)]
pub struct State<B: ModeBin> {
  n_processed: usize,
  bits_processed: usize,
  incomplete_bin: Option<B>,
  incomplete_reps: usize,
}

impl<B: ModeBin> State<B> {
  pub fn unchecked_limit_reps(&mut self, bin: B, full_reps: usize, limit: usize) -> usize {
    if full_reps > limit {
      self.incomplete_bin = Some(bin);
      self.incomplete_reps = full_reps - limit;
      limit
    } else {
      full_reps
    }
  }
}

pub trait NumDecompressor<U: UnsignedLike>: Debug {
  fn bits_remaining(&self) -> usize;

  fn decompress_unsigneds(
    &mut self,
    reader: &mut BitReader,
    error_on_insufficient_data: bool,
    dest: &mut [U],
  ) -> QCompressResult<Progress>;

  fn clone_inner(&self) -> Box<dyn NumDecompressor<U>>;
}

// NumDecompressor does the main work of decoding bytes into NumberLikes
#[derive(Clone, Debug)]
struct NumDecompressorImpl<U: UnsignedLike, M: Mode<U>> {
  // known information about the chunk
  mode: M,
  huffman_table: HuffmanTable<M::Bin>,
  n: usize,
  compressed_body_size: usize,
  max_bits_per_num_block: usize,
  max_overshoot_per_num_block: Bitlen,
  use_run_len: bool,

  // mutable state
  state: State<M::Bin>,
}

pub fn new<U: UnsignedLike>(
  data_page_meta: DataPageMetadata<U>,
) -> QCompressResult<Box<dyn NumDecompressor<U>>> {
  let DataPageMetadata {
    n,
    compressed_body_size,
    dyn_mode,
    bins,
  } = data_page_meta;
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
  let use_run_len = run_len_utils::use_run_len(bins);
  let res: Box<dyn NumDecompressor<U>> = match dyn_mode {
    DynMode::Classic => {
      let mode = ClassicMode;
      Box::new(NumDecompressorImpl {
        huffman_table: HuffmanTable::from_bins(&mode, bins),
        n,
        compressed_body_size,
        max_bits_per_num_block,
        max_overshoot_per_num_block,
        use_run_len,
        mode,
        state: State::default(),
      })
    }
    DynMode::Gcd => {
      let mode = GcdMode;
      Box::new(NumDecompressorImpl {
        huffman_table: HuffmanTable::from_bins(&mode, bins),
        n,
        compressed_body_size,
        max_bits_per_num_block,
        max_overshoot_per_num_block,
        use_run_len,
        mode,
        state: State::default(),
      })
    }
    DynMode::FloatMult { inv_base, .. } => {
      let mode = FloatMultMode::new(inv_base);
      Box::new(NumDecompressorImpl {
        huffman_table: HuffmanTable::from_bins(&mode, bins),
        n,
        compressed_body_size,
        max_bits_per_num_block,
        max_overshoot_per_num_block,
        use_run_len,
        mode,
        state: State::default(),
      })
    }
  };

  Ok(res)
}

impl<U: UnsignedLike, M: Mode<U>> NumDecompressor<U> for NumDecompressorImpl<U, M> {
  fn bits_remaining(&self) -> usize {
    self.compressed_body_size * 8 - self.state.bits_processed
  }

  // If hits a corruption, it returns an error and leaves reader and self unchanged.
  // State managed here: n_processed, bits_processed
  fn decompress_unsigneds(
    &mut self,
    reader: &mut BitReader,
    error_on_insufficient_data: bool,
    dest: &mut [U],
  ) -> QCompressResult<Progress> {
    let initial_reader = reader.clone();
    let initial_state = self.state.clone();
    let res = self.decompress_unsigneds_dirty(reader, error_on_insufficient_data, dest);
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

  fn clone_inner(&self) -> Box<dyn NumDecompressor<U>> {
    Box::new(self.clone())
  }
}

impl<U: UnsignedLike, M: Mode<U>> NumDecompressorImpl<U, M> {
  #[inline]
  fn unchecked_decompress_num_block<RunLenOp: RunLenOperator>(
    &mut self,
    reader: &mut BitReader,
    dest: &mut [U],
  ) -> usize {
    let bin = self.huffman_table.unchecked_search_with_reader(reader);
    RunLenOp::unchecked_decompress_for_bin::<U, M>(&mut self.state, reader, bin, self.mode, dest)
  }

  // returns count of numbers processed
  #[inline(never)]
  fn unchecked_decompress_num_blocks<RunLenOp: RunLenOperator>(
    &mut self,
    reader: &mut BitReader,
    mut guaranteed_safe_num_blocks: usize,
    batch_size: usize,
    n_processed: &mut usize,
    dest: &mut [U],
  ) {
    while guaranteed_safe_num_blocks > 0 && RunLenOp::batch_ongoing(*n_processed, batch_size) {
      *n_processed += self
        .unchecked_decompress_num_block::<RunLenOp>(reader, &mut dest[*n_processed..batch_size]);
      guaranteed_safe_num_blocks -= 1;
    }
  }

  fn decompress_num_block(
    &mut self,
    reader: &mut BitReader,
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
        let res = self.mode.decompress_unsigned(&bin.mode_bin, reader);
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
        self.state.incomplete_bin = Some(bin.mode_bin);
        self.state.incomplete_reps = full_reps;
        let reps = min(full_reps, dest.len());
        self.decompress_offsets(reader, &bin.mode_bin, reps, dest)?;
        self.state.incomplete_reps -= reps;
        Ok(reps)
      }
    }
  }

  // errors on insufficient data, but updates unsigneds with last complete number
  // and leaves reader at end end of last complete number
  fn decompress_offsets(
    &self,
    reader: &mut BitReader,
    bin: &M::Bin,
    reps: usize,
    dest: &mut [U],
  ) -> QCompressResult<()> {
    for i in 0..reps {
      let start_bit_idx = reader.bit_idx();
      let u = self.mode.decompress_unsigned(bin, reader);
      if u.is_err() {
        reader.seek_to(start_bit_idx);
      }
      dest[i] = u?;
    }

    Ok(())
  }

  // After much debugging a performance degradation from error handling changes,
  // it turned out this function's logic ran slower when inlining.
  // I don't understand why, but telling it not
  // to inline fixed the performance issue.
  // https://stackoverflow.com/questions/70911460/why-does-an-unrelated-heap-allocation-in-the-same-rust-scope-hurt-performance
  //
  // state managed here: incomplete_bin
  #[inline(never)]
  fn decompress_unsigneds_dirty(
    &mut self,
    reader: &mut BitReader,
    error_on_insufficient_data: bool,
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
      let constant_bin = self.huffman_table.unchecked_search_with_reader(reader);
      let constant = self
        .mode
        .decompress_unsigned(&constant_bin.mode_bin, reader)?;
      dest[0..batch_size].fill(constant);
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
      let incomplete_res = self.decompress_offsets(
        reader,
        self.state.incomplete_bin.as_ref().unwrap(),
        reps,
        dest,
      );
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
          self.unchecked_decompress_num_blocks::<GeneralRunLenOp>(
            reader,
            guaranteed_safe_num_blocks,
            batch_size,
            &mut res.n_processed,
            dest,
          );
        } else {
          self.unchecked_decompress_num_blocks::<TrivialRunLenOp>(
            reader,
            guaranteed_safe_num_blocks,
            batch_size,
            &mut res.n_processed,
            dest,
          );
        }
      } else {
        break;
      }
    }

    // do checked operations for the rest
    while res.n_processed < batch_size {
      res.n_processed += match self.decompress_num_block(reader, &mut dest[res.n_processed..]) {
        Ok(n_processed) => n_processed,
        Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => {
          return mark_insufficient(res, e)
        }
        Err(e) => return Err(e),
      };
    }

    Ok(res)
  }
}

impl<U: UnsignedLike> Clone for Box<dyn NumDecompressor<U>> {
  fn clone(&self) -> Self {
    self.clone_inner()
  }
}
