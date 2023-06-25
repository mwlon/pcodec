use std::cmp::min;
use std::fmt::Debug;

use crate::ans;
use crate::bin::BinDecompressionInfo;
use crate::bit_reader::BitReader;
use crate::chunk_metadata::DataPageMetadata;
use crate::constants::{Bitlen, DECOMPRESS_UNCHECKED_THRESHOLD};
use crate::data_types::UnsignedLike;
use crate::errors::{ErrorKind, QCompressError, QCompressResult};
use crate::modes::adjusted::AdjustedMode;
use crate::modes::classic::ClassicMode;
use crate::modes::gcd::GcdMode;
use crate::modes::DynMode;
use crate::modes::Mode;
use crate::progress::Progress;
use crate::unsigned_src_dst::UnsignedDst;

#[derive(Clone, Debug)]
pub struct State {
  n_processed: usize,
  bits_processed: usize,
  ans_decoder: ans::Decoder,
}

struct Backup {
  n_processed: usize,
  bits_processed: usize,
  ans_decoder_backup: usize,
}

impl State {
  fn backup(&self) -> Backup {
    Backup {
      n_processed: self.n_processed,
      bits_processed: self.bits_processed,
      ans_decoder_backup: self.ans_decoder.state,
    }
  }

  fn recover(&mut self, backup: Backup) {
    self.n_processed = backup.n_processed;
    self.bits_processed = backup.bits_processed;
    self.ans_decoder.state = backup.ans_decoder_backup;
  }
}

pub trait NumDecompressor<U: UnsignedLike>: Debug {
  fn bits_remaining(&self) -> usize;

  fn decompress_unsigneds(
    &mut self,
    reader: &mut BitReader,
    error_on_insufficient_data: bool,
    dst: UnsignedDst<U>,
  ) -> QCompressResult<Progress>;

  fn clone_inner(&self) -> Box<dyn NumDecompressor<U>>;
}

// NumDecompressor does the main work of decoding bytes into NumberLikes
#[derive(Clone, Debug)]
struct NumDecompressorImpl<U: UnsignedLike, M: Mode<U>> {
  // known information about the chunk
  infos: Vec<BinDecompressionInfo<U>>,
  mode: M,
  n: usize,
  delta_order: usize, // only used to infer how many extra 0's are at the end
  compressed_body_size: usize,
  max_bits_per_num_block: Bitlen,

  // mutable state
  state: State,
}

pub fn new<U: UnsignedLike>(
  data_page_meta: DataPageMetadata<U>,
) -> QCompressResult<Box<dyn NumDecompressor<U>>> {
  let DataPageMetadata {
    n,
    compressed_body_size,
    dyn_mode,
    bins,
    delta_moments,
    ans_size_log,
    ans_final_state,
  } = data_page_meta;
  let delta_order = delta_moments.order();
  if bins.is_empty() && n > delta_order {
    return Err(QCompressError::corruption(format!(
      "unable to decompress chunk with no bins and {} deltas",
      n - delta_order,
    )));
  }
  let ans_decoder = ans::Decoder::from_bins(ans_size_log, bins, ans_final_state)?;
  let adj_bits = dyn_mode.adjustment_bits();

  let max_bits_per_num_block = bins
    .iter()
    .map(|bin| {
      let max_ans_bits = ans_size_log - bin.weight.ilog2();
      max_ans_bits + bin.offset_bits + adj_bits
    })
    .max()
    .unwrap_or(Bitlen::MAX);
  let state = State {
    n_processed: 0,
    bits_processed: 0,
    ans_decoder,
  };
  let infos = bins
    .iter()
    .map(BinDecompressionInfo::from)
    .collect::<Vec<_>>();
  let res: Box<dyn NumDecompressor<U>> = match dyn_mode {
    DynMode::Classic => {
      let mode = ClassicMode;
      Box::new(NumDecompressorImpl {
        infos,
        n,
        delta_order,
        compressed_body_size,
        max_bits_per_num_block,
        mode,
        state,
      })
    }
    DynMode::Gcd => {
      let mode = GcdMode;
      Box::new(NumDecompressorImpl {
        infos,
        n,
        delta_order,
        compressed_body_size,
        max_bits_per_num_block,
        mode,
        state,
      })
    }
    DynMode::FloatMult { adj_bits, .. } => Box::new(NumDecompressorImpl {
      infos,
      n,
      delta_order,
      compressed_body_size,
      max_bits_per_num_block,
      mode: AdjustedMode::new(adj_bits),
      state,
    }),
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
    mut dst: UnsignedDst<U>,
  ) -> QCompressResult<Progress> {
    let initial_reader = reader.clone();
    let state_backup = self.state.backup();
    let res = self.decompress_unsigneds_dirty(reader, error_on_insufficient_data, &mut dst);
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
        self.state.recover(state_backup);
      }
    }
    res
  }

  fn clone_inner(&self) -> Box<dyn NumDecompressor<U>> {
    Box::new(self.clone())
  }
}

impl<U: UnsignedLike, M: Mode<U>> NumDecompressorImpl<U, M> {
  fn unchecked_decompress_num_block(&mut self, reader: &mut BitReader, dst: &mut UnsignedDst<U>) {
    let token = self.state.ans_decoder.unchecked_decode(reader);
    let bin = &self.infos[token as usize];
    let u = self.mode.unchecked_decompress_unsigned(bin, reader);
    dst.write_unsigned(u);
    if M::USES_ADJUSTMENT {
      dst.write_adj(self.mode.unchecked_decompress_adjustment(reader));
    }
    dst.incr();
  }

  // returns count of numbers processed
  #[inline(never)]
  fn unchecked_decompress_num_blocks(
    &mut self,
    reader: &mut BitReader,
    guaranteed_safe_num_blocks: usize,
    dst: &mut UnsignedDst<U>,
  ) {
    for _ in 0..guaranteed_safe_num_blocks {
      self.unchecked_decompress_num_block(reader, dst);
    }
  }

  fn decompress_num_block_dirty(
    &mut self,
    reader: &mut BitReader,
    dst: &mut UnsignedDst<U>,
  ) -> QCompressResult<()> {
    let token = self.state.ans_decoder.decode(reader)?;
    let bin = &self.infos[token as usize];
    let u = self.mode.decompress_unsigned(bin, reader)?;
    let adj = if M::USES_ADJUSTMENT {
      self.mode.decompress_adjustment(reader)?
    } else {
      U::ZERO
    };
    dst.write_unsigned(u);
    if M::USES_ADJUSTMENT {
      dst.write_adj(adj);
    }
    dst.incr();
    Ok(())
  }

  fn decompress_num_block(
    &mut self,
    reader: &mut BitReader,
    dst: &mut UnsignedDst<U>,
  ) -> QCompressResult<()> {
    let start_bit_idx = reader.bit_idx();
    let start_ans_state = self.state.ans_decoder.state;
    let res = self.decompress_num_block_dirty(reader, dst);
    if res.is_err() {
      reader.seek_to(start_bit_idx);
      self.state.ans_decoder.state = start_ans_state;
    }
    res
  }

  #[inline(never)]
  fn decompress_unsigneds_dirty(
    &mut self,
    reader: &mut BitReader,
    error_on_insufficient_data: bool,
    dst: &mut UnsignedDst<U>,
  ) -> QCompressResult<Progress> {
    let remaining = self.n - self.state.n_processed;
    let batch_size = min(remaining, dst.len());
    let delta_batch_size = min(
      remaining.saturating_sub(self.delta_order),
      dst.len(),
    );
    if batch_size == 0 {
      return Ok(Progress {
        finished_body: self.state.n_processed == self.n,
        ..Default::default()
      });
    }

    let mark_insufficient = |dst: &mut UnsignedDst<U>, e: QCompressError| {
      if error_on_insufficient_data {
        Err(e)
      } else {
        Ok(Progress {
          n_processed: dst.n_processed(),
          finished_body: false,
          insufficient_data: true,
        })
      }
    };

    // as long as there's enough compressed data available, we don't need checked operations
    let remaining_unsigneds = delta_batch_size - dst.n_processed();
    let guaranteed_safe_num_blocks = if self.max_bits_per_num_block == 0 {
      remaining_unsigneds
    } else {
      min(
        remaining_unsigneds,
        reader.bits_remaining() / self.max_bits_per_num_block as usize,
      )
    };
    if guaranteed_safe_num_blocks >= DECOMPRESS_UNCHECKED_THRESHOLD {
      // don't slow down the tight loops with runtime checks - do these upfront to choose
      // the best compiled tight loop
      self.unchecked_decompress_num_blocks(reader, guaranteed_safe_num_blocks, dst);
    }

    // do checked operations for the rest
    while dst.n_processed() < delta_batch_size {
      match self.decompress_num_block(reader, dst) {
        Ok(()) => (),
        Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => {
          return mark_insufficient(dst, e)
        }
        Err(e) => return Err(e),
      };
    }

    // handle trailing adjustments
    while dst.n_processed() < batch_size {
      if M::USES_ADJUSTMENT {
        match self.mode.decompress_adjustment(reader) {
          Ok(adj) => dst.write_adj(adj),
          Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => {
            return mark_insufficient(dst, e)
          }
          Err(e) => return Err(e),
        }
      }
      dst.incr();
    }

    Ok(Progress {
      n_processed: dst.n_processed(),
      finished_body: batch_size >= self.n - self.state.n_processed,
      ..Default::default()
    })
  }
}

impl<U: UnsignedLike> Clone for Box<dyn NumDecompressor<U>> {
  fn clone(&self) -> Self {
    self.clone_inner()
  }
}
