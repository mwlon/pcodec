use std::cmp::{max, min};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem;
use std::mem::MaybeUninit;

use crate::ans;
use crate::bin::BinDecompressionInfo;
use crate::bit_reader::BitReader;
use crate::chunk_metadata::DataPageMetadata;
use crate::constants::{Bitlen, DECOMPRESS_UNCHECKED_THRESHOLD, MAX_DELTA_ENCODING_ORDER};
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

impl<const STREAMS: usize> State<STREAMS> {
  fn backup(&self) -> Backup<STREAMS> {
    Backup {
      n_processed: self.n_processed,
      bits_processed: self.bits_processed,
      ans_decoder_backups: self.ans_decoders.iter().map(|decoder| decoder.state).collect(),
    }
  }

  fn recover(&mut self, backup: Backup<STREAMS>) {
    self.n_processed = backup.n_processed;
    self.bits_processed = backup.bits_processed;
    self.ans_decoders.state = backup.ans_decoder_backups;
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

#[derive(Clone, Debug)]
struct StreamConfig<U: UnsignedLike> {
  infos: Vec<BinDecompressionInfo<U>>,
  delta_order: usize, // only used to infer how many extra 0's are at the end
}

// NumDecompressor does the main work of decoding bytes into NumberLikes
#[derive(Clone, Debug)]
struct NumDecompressorImpl<U: UnsignedLike, M: Mode<U>, const STREAMS: usize> {
  // known information about the chunk
  n: usize,
  compressed_body_size: usize,
  max_bits_per_num_block: Bitlen,
  stream_configs: [StreamConfig<U>; STREAMS],
  phantom: PhantomData<M>,

  // mutable state
  state: State<STREAMS>,
}

pub fn new<U: UnsignedLike>(
  data_page_meta: DataPageMetadata<U>,
) -> QCompressResult<Box<dyn NumDecompressor<U>>> {
  let mut max_bits_per_num_block = 0;
  for stream in &data_page_meta.streams {
    max_bits_per_num_block += stream.bins
      .iter()
      .map(|bin| {
        let max_ans_bits = stream.ans_size_log - bin.weight.ilog2();
        max_ans_bits + bin.offset_bits
      })
      .max()
      .unwrap_or(Bitlen::MAX);
  }
  let res: Box<dyn NumDecompressor<U>> = match dyn_mode {
    DynMode::Classic => Box::new(NumDecompressorImpl::<U, ClassicMode, 1>::new(data_page_meta, max_bits_per_num_block)?),
    DynMode::Gcd => Box::new(NumDecompressorImpl::<U, GcdMode, 1>::new(data_page_meta, max_bits_per_num_block)?),
    DynMode::FloatMult { .. } => Box::new(NumDecompressorImpl::<U, ClassicMode, 2>::new(data_page_meta, max_bits_per_num_block)?),
  };

  Ok(res)
}

impl<U: UnsignedLike, M: Mode<U>, const STREAMS: usize> NumDecompressor<U> for NumDecompressorImpl<U, M, STREAMS> {
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
          reader.drain_empty_byte("nonzero bits in end of final byte of data page numbers")?;
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

impl<U: UnsignedLike, M: Mode<U>, const STREAMS: usize> NumDecompressorImpl<U, M, STREAMS> {
  fn new(data_page_meta: DataPageMetadata<U>, max_bits_per_num_block: Bitlen) -> QCompressResult<Self> {
    let DataPageMetadata {
      n,
      compressed_body_size,
      streams,
      ..
    } = data_page_meta;

    let mut configs: [MaybeUninit<StreamConfig<U>>; STREAMS] = unsafe { MaybeUninit::uninit().assume_init() };
    let mut decoders: [MaybeUninit<ans::Decoder>; STREAMS] = unsafe { MaybeUninit::uninit().assume_init() };
    for i in 0..STREAMS {
      let stream = &streams[i];

      let delta_order = stream.delta_moments.order();
      if stream.bins.is_empty() && n > delta_order {
        return Err(QCompressError::corruption(format!(
          "unable to decompress chunk with no bins and {} deltas",
          n - delta_order,
        )));
      }

      decoders[i].write(ans::Decoder::from_stream_meta(stream)?);

      let infos = stream.bins
        .iter()
        .map(BinDecompressionInfo::from)
        .collect::<Vec<_>>();
      configs[i].write(StreamConfig {
        infos,
        delta_order,
      });
    }

    Ok(Self {
      n,
      compressed_body_size,
      max_bits_per_num_block,
      stream_configs: unsafe { mem::transmute(configs) },
      phantom: PhantomData,
      state: State {
        n_processed: 0,
        bits_processed: 0,
        ans_decoders: unsafe { mem::transmute(decoders) },
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
    batch_size: usize,
    dst: &mut UnsignedDst<U>,
  ) -> QCompressResult<()> {
    for stream_idx in 0..STREAMS {
      let config = &self.stream_configs[stream_idx];
      if dst.n_processed() + config.delta_order >= batch_size {
        continue;
      }
      let token = self.state.ans_decoders[stream_idx].decode(reader)?;
      let bin = &config.infos[token as usize];
      let u = M::decompress_unsigned(bin, reader)?;
      dst.write(stream_idx, u);
    }
    dst.incr();
    Ok(())
  }

  fn decompress_num_block(
    &mut self,
    reader: &mut BitReader,
    batch_size: usize,
    dst: &mut UnsignedDst<U>,
  ) -> QCompressResult<()> {
    let start_bit_idx = reader.bit_idx();
    let start_ans_state = self.state.ans_decoders.state;
    let res = self.decompress_num_block_dirty(reader, batch_size, dst);
    if res.is_err() {
      reader.seek_to(start_bit_idx);
      self.state.ans_decoders.state = start_ans_state;
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
      remaining.saturating_sub(MAX_DELTA_ENCODING_ORDER),
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
