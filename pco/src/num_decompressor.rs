use std::cmp::min;
use std::fmt::Debug;
use std::marker::PhantomData;

use std::mem::MaybeUninit;

use crate::bin::BinDecompressionInfo;
use crate::bit_reader::BitReader;
use crate::chunk_metadata::DataPageMetadata;
use crate::constants::{Bitlen, DECOMPRESS_UNCHECKED_THRESHOLD, MAX_DELTA_ENCODING_ORDER, MAX_N_STREAMS};
use crate::data_types::UnsignedLike;
use crate::errors::{ErrorKind, PcoError, PcoResult};
use crate::modes::classic::ClassicMode;
use crate::modes::gcd::GcdMode;
use crate::modes::{ConstMode, gcd};
use crate::progress::Progress;
use crate::unsigned_src_dst::UnsignedDst;
use crate::{ans, Mode};

#[derive(Clone, Debug)]
pub struct State<const STREAMS: usize> {
  n_processed: usize,
  bits_processed: usize,
  ans_decoders: [ans::Decoder; STREAMS],
}

struct Backup<const STREAMS: usize> {
  n_processed: usize,
  bits_processed: usize,
  ans_decoder_backups: [usize; STREAMS],
}

fn decoder_states<const STREAMS: usize>(decoders: &[ans::Decoder; STREAMS]) -> [usize; STREAMS] {
  core::array::from_fn(|stream_idx| decoders[stream_idx].state)
}

fn recover_decoders<const STREAMS: usize>(
  backups: [usize; STREAMS],
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

pub trait NumDecompressor<U: UnsignedLike>: Debug {
  fn bits_remaining(&self) -> usize;

  fn initial_value_required(&self, stream_idx: usize) -> Option<U>;

  fn decompress_unsigneds(
    &mut self,
    reader: &mut BitReader,
    error_on_insufficient_data: bool,
    dst: UnsignedDst<U>,
  ) -> PcoResult<Progress>;

  fn clone_inner(&self) -> Box<dyn NumDecompressor<U>>;
}

#[derive(Clone, Debug)]
struct StreamConfig<U: UnsignedLike> {
  infos: Vec<BinDecompressionInfo<U>>,
  delta_order: usize, // only used to infer how many extra 0's are at the end
}

// NumDecompressor does the main work of decoding bytes into NumberLikes
#[derive(Clone, Debug)]
struct NumDecompressorImpl<U: UnsignedLike, M: ConstMode<U>, const STREAMS: usize> {
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

pub fn new<U: UnsignedLike>(
  data_page_meta: DataPageMetadata<U>,
) -> PcoResult<Box<dyn NumDecompressor<U>>> {
  let mut max_bits_per_num_block = 0;
  for stream in &data_page_meta.streams {
    max_bits_per_num_block += stream
      .bins
      .iter()
      .map(|bin| {
        let max_ans_bits = stream.ans_size_log - bin.weight.ilog2();
        max_ans_bits + bin.offset_bits
      })
      .max()
      .unwrap_or(Bitlen::MAX);
  }

  let (needs_gcd, n_streams) = match data_page_meta.mode {
    Mode::Classic | Mode::Gcd => {
      let stream_meta = &data_page_meta.streams[0];
      if stream_meta.is_trivial() {
        (false, 0)
      } else {
        let needs_gcd = gcd::use_gcd_arithmetic(&data_page_meta.streams[0].bins);
        (needs_gcd, 1)
      }
    },
    Mode::FloatMult(_) => {
      let n_streams = if data_page_meta.streams[1].is_trivial() {
        if data_page_meta.streams[0].is_trivial() {
          0
        } else {
          1
        }
      } else {
        2
      };
      (false, n_streams)
    }
  };
  let mut initial_values_required = [None; MAX_N_STREAMS];
  for stream_idx in n_streams..MAX_N_STREAMS {
    initial_values_required[stream_idx] = data_page_meta.streams.get(stream_idx)
      .and_then(|stream_meta| stream_meta.bins.get(0))
      .map(|only_bin| only_bin.lower);
  }

  let res: Box<dyn NumDecompressor<U>> = match (needs_gcd, n_streams) {
    (false, 0) => Box::new(
      NumDecompressorImpl::<U, ClassicMode, 0>::new(
        data_page_meta,
        max_bits_per_num_block,
        initial_values_required,
      )?,
    ),
    (false, 1) => Box::new(
      NumDecompressorImpl::<U, ClassicMode, 1>::new(
        data_page_meta,
        max_bits_per_num_block,
        initial_values_required,
      )?,
    ),
    (true, 1) => Box::new(NumDecompressorImpl::<U, GcdMode, 1>::new(
      data_page_meta,
      max_bits_per_num_block,
      initial_values_required,
    )?),
    (false, 2) => Box::new(
      NumDecompressorImpl::<U, ClassicMode, 2>::new(
        data_page_meta,
        max_bits_per_num_block,
        initial_values_required,
      )?,
    ),
    _ => panic!("unknown decompression implementation; should be unreachable")
  };

  Ok(res)
}

impl<U: UnsignedLike, M: ConstMode<U>, const STREAMS: usize> NumDecompressor<U>
  for NumDecompressorImpl<U, M, STREAMS>
{
  fn bits_remaining(&self) -> usize {
    self.compressed_body_size * 8 - self.state.bits_processed
  }

  fn initial_value_required(&self, stream_idx: usize) -> Option<U> {
    self.initial_values_required[stream_idx]
  }

  // If hits a corruption, it returns an error and leaves reader and self unchanged.
  // State managed here: n_processed, bits_processed
  fn decompress_unsigneds(
    &mut self,
    reader: &mut BitReader,
    error_on_insufficient_data: bool,
    mut dst: UnsignedDst<U>,
  ) -> PcoResult<Progress> {
    let initial_reader = reader.clone();
    let state_backup = self.state.backup();
    let res = self.decompress_unsigneds_dirty(reader, error_on_insufficient_data, &mut dst);
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

  fn clone_inner(&self) -> Box<dyn NumDecompressor<U>> {
    Box::new(self.clone())
  }
}

impl<U: UnsignedLike, M: ConstMode<U>, const STREAMS: usize> NumDecompressorImpl<U, M, STREAMS> {
  fn new(
    data_page_meta: DataPageMetadata<U>,
    max_bits_per_num_block: Bitlen,
    initial_values_required: [Option<U>; MAX_N_STREAMS],
  ) -> PcoResult<Self> {
    let DataPageMetadata {
      n,
      compressed_body_size,
      streams,
      ..
    } = data_page_meta;

    let mut decoders: [MaybeUninit<ans::Decoder>; STREAMS] =
      unsafe { MaybeUninit::uninit().assume_init() };
    for i in 0..STREAMS {
      let stream = &streams[i];

      let delta_order = stream.delta_moments.order();
      if stream.bins.is_empty() && n > delta_order {
        return Err(PcoError::corruption(format!(
          "unable to decompress chunk with no bins and {} deltas",
          n - delta_order,
        )));
      }

      decoders[i].write(ans::Decoder::from_stream_meta(stream)?);
    }

    let stream_configs = core::array::from_fn(|stream_idx| {
      let stream = &streams[stream_idx];
      let infos = stream
        .bins
        .iter()
        .map(BinDecompressionInfo::from)
        .collect::<Vec<_>>();
      StreamConfig {
        infos,
        delta_order: stream.delta_moments.order(),
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
      },
    })
  }

  fn unchecked_decompress_num_block(&mut self, reader: &mut BitReader, dst: &mut UnsignedDst<U>) {
    for stream_idx in 0..STREAMS {
      let token = self.state.ans_decoders[stream_idx].unchecked_decode(reader);
      let bin = &self.stream_configs[stream_idx].infos[token as usize];
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
    for stream_idx in 0..STREAMS {
      let config = &self.stream_configs[stream_idx];
      if dst.n_processed() + config.delta_order < self.n - self.state.n_processed {
        let token = self.state.ans_decoders[stream_idx].decode(reader)?;
        let bin = &config.infos[token as usize];
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

impl<U: UnsignedLike> Clone for Box<dyn NumDecompressor<U>> {
  fn clone(&self) -> Self {
    self.clone_inner()
  }
}
