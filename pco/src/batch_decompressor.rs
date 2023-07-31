use std::cmp::min;
use std::fmt::Debug;
use std::marker::PhantomData;

use std::mem::MaybeUninit;

use crate::ans::AnsState;
use crate::ans::Token;
use crate::bin::BinDecompressionInfo;
use crate::bit_reader::BitReader;
use crate::chunk_metadata::PageMetadata;
use crate::constants::{
  Bitlen, DECOMPRESS_UNCHECKED_THRESHOLD, MAX_DELTA_ENCODING_ORDER, MAX_LOOKBACK, MAX_N_STREAMS,
};
use crate::data_types::UnsignedLike;
use crate::errors::{ErrorKind, PcoError, PcoResult};
use crate::lookback::Lookback;
use crate::modes::classic::ClassicMode;
use crate::modes::gcd::GcdMode;
use crate::modes::ConstMode;
use crate::progress::Progress;
use crate::unsigned_src_dst::UnsignedDst;
use crate::{ans, ChunkMetadata};

#[derive(Clone, Debug)]
pub struct State<const STREAMS: usize> {
  n_processed: usize,
  bits_processed: usize,
  ans_decoders: [ans::Decoder; STREAMS],
  past_bin_idxs: [[u32; STREAMS]; MAX_LOOKBACK],
}

struct Backup<const STREAMS: usize> {
  n_processed: usize,
  bits_processed: usize,
  ans_decoder_backups: [AnsState; STREAMS],
}

fn decoder_states<const STREAMS: usize>(decoders: &[ans::Decoder; STREAMS]) -> [AnsState; STREAMS] {
  core::array::from_fn(|stream_idx| decoders[stream_idx].state)
}

fn recover_decoders<const STREAMS: usize>(
  backups: [AnsState; STREAMS],
  decoders: &mut [ans::Decoder; STREAMS],
) {
  for stream_idx in 0..STREAMS {
    decoders[stream_idx].state = backups[stream_idx];
  }
}

impl<const STREAMS: usize> State<STREAMS> {
  fn backup(&self) -> Backup<STREAMS> {
    Backup {
      n_processed: self.n_processed,
      bits_processed: self.bits_processed,
      ans_decoder_backups: decoder_states(&self.ans_decoders),
    }
  }

  fn recover(&mut self, backup: Backup<STREAMS>) {
    self.n_processed = backup.n_processed;
    self.bits_processed = backup.bits_processed;
    recover_decoders(
      backup.ans_decoder_backups,
      &mut self.ans_decoders,
    );
  }
}

pub trait BatchDecompressor<U: UnsignedLike>: Debug {
  fn bits_remaining(&self) -> usize;

  fn decompress_unsigneds(
    &mut self,
    reader: &mut BitReader,
    error_on_insufficient_data: bool,
    dst: &mut UnsignedDst<U>,
  ) -> PcoResult<Progress>;

  fn clone_inner(&self) -> Box<dyn BatchDecompressor<U>>;
}

#[derive(Clone, Debug)]
struct StreamConfig<U: UnsignedLike> {
  infos: Vec<BinDecompressionInfo<U>>,
  lookbacks: Vec<Lookback>,
  delta_order: usize, // only used to infer how many extra 0's are at the end
}

// BatchDecompressor does the main work of decoding bytes into UnsignedLikes
#[derive(Clone, Debug)]
struct BatchDecompressorImpl<U: UnsignedLike, M: ConstMode<U>, const STREAMS: usize> {
  // known information about the chunk
  n: usize,
  compressed_body_size: usize,
  max_bits_per_num_block: Bitlen,
  stream_configs: [StreamConfig<U>; STREAMS],
  phantom: PhantomData<M>,
  initial_values_required: [Option<U>; MAX_N_STREAMS],

  // mutable state
  state: State<STREAMS>,
}

struct BatchDecompressorInputs<'a, U: UnsignedLike> {
  n: usize,
  compressed_body_size: usize,
  chunk_meta: &'a ChunkMetadata<U>,
  page_meta: PageMetadata<U>,
  max_bits_per_num_block: Bitlen,
  initial_values_required: [Option<U>; MAX_N_STREAMS],
}

#[allow(clippy::needless_range_loop)]
pub fn new<U: UnsignedLike>(
  n: usize,
  compressed_body_size: usize,
  chunk_meta: &ChunkMetadata<U>,
  page_meta: PageMetadata<U>,
) -> PcoResult<Box<dyn BatchDecompressor<U>>> {
  let mut max_bits_per_num_block = 0;
  for stream in &chunk_meta.streams {
    max_bits_per_num_block += stream
      .bins
      .iter()
      .map(|bin| {
        let max_ans_bits = stream.ans_size_log - bin.weight.ilog2() as Bitlen;
        max_ans_bits + bin.offset_bits
      })
      .max()
      .unwrap_or(Bitlen::MAX);
  }

  let (needs_gcd, n_streams) = chunk_meta.nontrivial_gcd_and_n_streams();

  let mut initial_values_required = [None; MAX_N_STREAMS];
  for stream_idx in n_streams..MAX_N_STREAMS {
    initial_values_required[stream_idx] = chunk_meta
      .streams
      .get(stream_idx)
      .and_then(|stream_meta| stream_meta.bins.get(0))
      .map(|only_bin| only_bin.lower);
  }
  let inputs = BatchDecompressorInputs {
    n,
    compressed_body_size,
    chunk_meta,
    page_meta,
    max_bits_per_num_block,
    initial_values_required,
  };

  let res: Box<dyn BatchDecompressor<U>> = match (needs_gcd, n_streams) {
    (false, 0) => Box::new(BatchDecompressorImpl::<U, ClassicMode, 0>::new(inputs)?),
    (false, 1) => Box::new(BatchDecompressorImpl::<U, ClassicMode, 1>::new(inputs)?),
    (true, 1) => Box::new(BatchDecompressorImpl::<U, GcdMode, 1>::new(
      inputs,
    )?),
    (false, 2) => Box::new(BatchDecompressorImpl::<U, ClassicMode, 2>::new(inputs)?),
    _ => panic!("unknown decompression implementation; should be unreachable"),
  };

  Ok(res)
}

impl<U: UnsignedLike, M: ConstMode<U>, const STREAMS: usize> BatchDecompressor<U>
  for BatchDecompressorImpl<U, M, STREAMS>
{
  fn bits_remaining(&self) -> usize {
    self.compressed_body_size * 8 - self.state.bits_processed
  }

  // If hits a corruption, it returns an error and leaves reader and self unchanged.
  // State managed here: n_processed, bits_processed
  fn decompress_unsigneds(
    &mut self,
    reader: &mut BitReader,
    error_on_insufficient_data: bool,
    dst: &mut UnsignedDst<U>,
  ) -> PcoResult<Progress> {
    let initial_reader = reader.clone();
    let state_backup = self.state.backup();
    let res = self.decompress_unsigneds_dirty(reader, error_on_insufficient_data, dst);
    match &res {
      Ok(progress) => {
        self.state.n_processed += progress.n_processed;

        if progress.finished_body {
          reader.drain_empty_byte("nonzero bits in end of final byte of data page numbers")?;
        }
        self.state.bits_processed += reader.bit_idx() - initial_reader.bit_idx();
        if progress.finished_body {
          let compressed_body_bit_size = self.compressed_body_size * 8;
          if compressed_body_bit_size != self.state.bits_processed {
            return Err(PcoError::corruption(format!(
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

  fn clone_inner(&self) -> Box<dyn BatchDecompressor<U>> {
    Box::new(self.clone())
  }
}

impl<U: UnsignedLike, M: ConstMode<U>, const STREAMS: usize> BatchDecompressorImpl<U, M, STREAMS> {
  fn new(inputs: BatchDecompressorInputs<U>) -> PcoResult<Self> {
    let BatchDecompressorInputs {
      n,
      compressed_body_size,
      chunk_meta,
      page_meta,
      max_bits_per_num_block,
      initial_values_required,
    } = inputs;
    let mut decoders: [MaybeUninit<ans::Decoder>; STREAMS] =
      unsafe { MaybeUninit::uninit().assume_init() };

    let delta_orders = page_meta
      .streams
      .iter()
      .map(|stream| stream.delta_moments.order())
      .collect::<Vec<_>>();
    for stream_idx in 0..STREAMS {
      let chunk_stream = &chunk_meta.streams[stream_idx];
      let page_stream = &page_meta.streams[stream_idx];

      let delta_order = delta_orders[stream_idx];
      if chunk_stream.bins.is_empty() && n > delta_order {
        return Err(PcoError::corruption(format!(
          "unable to decompress chunk with no bins and {} deltas",
          n - delta_order,
        )));
      }

      decoders[stream_idx].write(ans::Decoder::from_stream_meta(
        chunk_stream,
        page_stream.ans_final_state,
      )?);
    }

    let stream_configs = core::array::from_fn(|stream_idx| {
      let stream = &chunk_meta.streams[stream_idx];
      let infos = stream
        .bins
        .iter()
        .map(BinDecompressionInfo::from)
        .collect::<Vec<_>>();
      let lookbacks = stream
        .lookbacks
        .iter()
        .map(|meta| meta.lookback)
        .collect::<Vec<_>>();
      StreamConfig {
        infos,
        lookbacks,
        delta_order: delta_orders[stream_idx],
      }
    });

    Ok(Self {
      n,
      compressed_body_size,
      max_bits_per_num_block,
      stream_configs,
      initial_values_required,
      phantom: PhantomData,
      state: State {
        n_processed: 0,
        bits_processed: 0,
        ans_decoders: decoders.map(|decoder| unsafe { MaybeUninit::assume_init(decoder) }),
        past_bin_idxs: [[0; STREAMS]; MAX_LOOKBACK],
      },
    })
  }

  #[inline]
  fn bin_idx(&self, n_processed: usize, token: Token, stream_idx: usize) -> Token {
    let config = &self.stream_configs[stream_idx];
    let n_bins = config.infos.len() as Token;
    if token < n_bins {
      token
    } else {
      let lookback = config.lookbacks[(token - n_bins) as usize];
      self.state.past_bin_idxs[n_processed.wrapping_sub(lookback as usize) % MAX_LOOKBACK]
        [stream_idx]
    }
  }

  fn unchecked_decompress_num_block(&mut self, reader: &mut BitReader, dst: &mut UnsignedDst<U>) {
    let n_processed = self.state.n_processed + dst.n_processed();
    for stream_idx in 0..STREAMS {
      let token = self.state.ans_decoders[stream_idx].unchecked_decode(reader);
      let bin_idx = self.bin_idx(n_processed, token, stream_idx);
      self.state.past_bin_idxs[n_processed % MAX_LOOKBACK][stream_idx] = bin_idx;
      let bin = &self.stream_configs[stream_idx].infos[bin_idx as usize];
      let u = M::unchecked_decompress_unsigned(bin, reader);
      dst.write(stream_idx, u);
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
  ) -> PcoResult<()> {
    let n_processed = self.state.n_processed + dst.n_processed();
    for stream_idx in 0..STREAMS {
      let config = &self.stream_configs[stream_idx];
      if n_processed + config.delta_order < self.n {
        let token = self.state.ans_decoders[stream_idx].decode(reader)?;
        let bin_idx = self.bin_idx(n_processed, token, stream_idx);
        self.state.past_bin_idxs[n_processed % MAX_LOOKBACK][stream_idx] = bin_idx;
        let bin = &config.infos[bin_idx as usize];
        let u = M::decompress_unsigned(bin, reader)?;
        dst.write(stream_idx, u);
      }
    }
    dst.incr();
    Ok(())
  }

  fn decompress_num_block(
    &mut self,
    reader: &mut BitReader,
    _batch_size: usize,
    dst: &mut UnsignedDst<U>,
  ) -> PcoResult<()> {
    let start_bit_idx = reader.bit_idx();
    let decoder_backups = decoder_states::<STREAMS>(&self.state.ans_decoders);
    let res = self.decompress_num_block_dirty(reader, dst);
    if res.is_err() {
      reader.seek_to(start_bit_idx);
      recover_decoders::<STREAMS>(decoder_backups, &mut self.state.ans_decoders);
    }
    res
  }

  fn fill_dst_if_needed(&self, dst: &mut UnsignedDst<U>) {
    for (stream_idx, maybe_initial_value) in self.initial_values_required.iter().enumerate() {
      if let Some(initial_value) = maybe_initial_value {
        dst.stream(stream_idx).fill(*initial_value);
      }
    }
  }

  #[inline(never)]
  fn decompress_unsigneds_dirty(
    &mut self,
    reader: &mut BitReader,
    error_on_insufficient_data: bool,
    dst: &mut UnsignedDst<U>,
  ) -> PcoResult<Progress> {
    let remaining = self.n - self.state.n_processed;
    let batch_size = min(remaining, dst.len());
    let delta_batch_size = min(
      remaining.saturating_sub(MAX_DELTA_ENCODING_ORDER),
      dst.len(),
    );
    if batch_size == 0 {
      return Ok(Progress {
        finished_body: self.state.n_processed == self.n,
        ..Default::default()
      });
    }

    self.fill_dst_if_needed(dst);

    let mark_insufficient = |dst: &mut UnsignedDst<U>, e: PcoError| {
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
    let remaining_full_blocks = delta_batch_size - dst.n_processed();
    let guaranteed_safe_num_blocks = if self.max_bits_per_num_block == 0 {
      remaining_full_blocks
    } else {
      min(
        remaining_full_blocks,
        reader.bits_remaining() / self.max_bits_per_num_block as usize,
      )
    };
    if guaranteed_safe_num_blocks >= DECOMPRESS_UNCHECKED_THRESHOLD {
      // don't slow down the tight loops with runtime checks - do these upfront to choose
      // the best compiled tight loop
      self.unchecked_decompress_num_blocks(reader, guaranteed_safe_num_blocks, dst);
    }

    // do checked operations for the rest
    while dst.n_processed() < batch_size {
      match self.decompress_num_block(reader, batch_size, dst) {
        Ok(()) => (),
        Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => {
          return mark_insufficient(dst, e)
        }
        Err(e) => return Err(e),
      };
    }

    Ok(Progress {
      n_processed: dst.n_processed(),
      finished_body: batch_size >= self.n - self.state.n_processed,
      ..Default::default()
    })
  }
}

impl<U: UnsignedLike> Clone for Box<dyn BatchDecompressor<U>> {
  fn clone(&self) -> Self {
    self.clone_inner()
  }
}
