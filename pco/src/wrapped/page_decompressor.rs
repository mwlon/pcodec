use std::cmp::min;
use std::fmt::Debug;
use std::marker::PhantomData;

use better_io::BetterBufRead;

use crate::bit_reader::{BitReader, BitReaderBuilder};
use crate::constants::{Lookback, FULL_BATCH_N, MAX_LZ_DELTA_LOOKBACK, PAGE_PADDING};
use crate::data_types::{Latent, NumberLike};
use crate::delta;
use crate::errors::{PcoError, PcoResult};
use crate::latent_batch_decompressor::LatentBatchDecompressor;
use crate::metadata::delta_encoding::{DeltaEncoding, DeltaMoments};
use crate::metadata::page_meta::PageMeta;
use crate::progress::Progress;
use crate::{bit_reader, ChunkMeta, Mode};

const PERFORMANT_BUF_READ_CAPACITY: usize = 8192;

// NOTE: in multiple places here, we use the fact that secondary latents
// are never delta encoded.

enum DeltaState<L: Latent> {
  None,
  Consecutive(DeltaMoments<L>),
  Lz {
    lookback_bd: LatentBatchDecompressor<Lookback>,
    lookbacks: [Lookback; MAX_LZ_DELTA_LOOKBACK],
    window: [L; MAX_LZ_DELTA_LOOKBACK],
  },
}

#[derive(Clone, Debug)]
pub struct State<L: Latent> {
  n_processed: usize,
  latent_batch_decompressors: Vec<LatentBatchDecompressor<L>>,
  primary_delta_state: DeltaState<L>,
  primary_latents: [L; FULL_BATCH_N],
  secondary_latents: [L; FULL_BATCH_N],
}

/// Holds metadata about a page and supports decompression.
pub struct PageDecompressor<T: NumberLike, R: BetterBufRead> {
  // immutable
  n: usize,
  mode: Mode<T::L>,
  maybe_constant_secondary: Option<T::L>,
  phantom: PhantomData<T>,

  // mutable
  reader_builder: BitReaderBuilder<R>,
  state: State<T::L>,
}

unsafe fn decompress_latents_w_delta<L: Latent>(
  reader: &mut BitReader,
  delta_state: &mut DeltaState<L>,
  latent_bd: &mut LatentBatchDecompressor<L>,
  dst: &mut [L],
  n_remaining: usize,
) -> PcoResult<()> {
  match delta_state {
    DeltaState::None => {
      latent_bd.decompress_latent_batch(reader, dst)?;
    }
    DeltaState::Consecutive(moments) => {
      let n_remaining_pre_delta = n_remaining.saturating_sub(moments.order());
      let pre_delta_len = if dst.len() <= n_remaining_pre_delta {
        dst.len()
      } else {
        // If we're at the end, LatentBatchdDecompressor won't initialize the last
        // few elements before delta decoding them, so we do that manually here to
        // satisfy MIRI. This step isn't really necessary.
        dst[n_remaining_pre_delta..].fill(L::default());
        n_remaining_pre_delta
      };
      latent_bd.decompress_latent_batch(reader, &mut dst[..pre_delta_len])?;
      delta::consecutive_decode_in_place(moments, dst);
    }
    DeltaState::Lz {
      lookback_bd,
      lookbacks,
      window,
    } => {
      lookback_bd.decompress_latent_batch(reader, lookbacks)?;
      latent_bd.decompress_latent_batch(reader, dst)?;
      delta::lz_decode_in_place(window, lookbacks, dst);
      if dst.len() >= MAX_LZ_DELTA_LOOKBACK {
        window.copy_from_slice(&dst[dst.len() - MAX_LZ_DELTA_LOOKBACK..]);
      }
    }
  }
  Ok(())
}

fn convert_from_latents_transmutable<T: NumberLike>(dst: &mut [T]) {
  // we wrote the joined latents to dst, so we can convert them in place
  for l_and_dst in dst {
    *l_and_dst = T::from_latent_ordered(l_and_dst.transmute_to_latent());
  }
}

fn convert_from_latents_nontransmutable<T: NumberLike>(primary: &[T::L], dst: &mut [T]) {
  // we wrote the joined latents to primary, so we need to move them over
  for (&l, dst) in primary.iter().zip(dst.iter_mut()) {
    *dst = T::from_latent_ordered(l);
  }
}

impl<T: NumberLike, R: BetterBufRead> PageDecompressor<T, R> {
  pub(crate) fn new(mut src: R, chunk_meta: &ChunkMeta<T::L>, n: usize) -> PcoResult<Self> {
    bit_reader::ensure_buf_read_capacity(&mut src, PERFORMANT_BUF_READ_CAPACITY);
    let mut reader_builder = BitReaderBuilder::new(src, PAGE_PADDING, 0);

    let page_meta = reader_builder
      .with_reader(|reader| unsafe { PageMeta::<T::L>::parse_from(reader, chunk_meta) })?;

    let mode = chunk_meta.mode;
    let primary_delta_state = match &chunk_meta.delta_encoding {
      DeltaEncoding::None => DeltaState::None,
      DeltaEncoding::Consecutive { order: _ } => {
        DeltaState::Consecutive(page_meta.per_latent_var[0].delta_moments.clone())
      }
      DeltaEncoding::Lz(var_meta) => DeltaState::Lz {
        window: delta::get_default_lz_window(),
        lookbacks: [0; MAX_LZ_DELTA_LOOKBACK],
        lookback_bd: LatentBatchDecompressor::new(var_meta, &page_meta.per_latent_var)?, // TODO
      },
    };

    let mut latent_batch_decompressors = Vec::new();
    for latent_var_idx in 0..mode.n_latent_vars() {
      let chunk_latent_meta = &chunk_meta.per_latent_var[latent_var_idx];
      let n_implicit_latents = chunk_meta.n_delta_moments(latent_var_idx);
      if chunk_latent_meta.bins.is_empty() && n > n_implicit_latents {
        return Err(PcoError::corruption(format!(
          "unable to decompress chunk with no bins and {} explicit latents",
          n - n_implicit_latents,
        )));
      }

      latent_batch_decompressors.push(LatentBatchDecompressor::new(
        chunk_latent_meta,
        &page_meta.per_latent_var[latent_var_idx],
      )?);
    }

    let maybe_constant_secondary = if latent_batch_decompressors.len() >= 2 {
      // This case relies on the fact that seconary latents are never delta encoded
      latent_batch_decompressors[1].maybe_constant_value
    } else {
      None
    };

    let secondary_default = maybe_constant_secondary.unwrap_or(T::L::default());

    // we don't store the whole ChunkMeta because it can get large due to bins
    Ok(Self {
      n,
      mode,
      maybe_constant_secondary,
      phantom: PhantomData,
      reader_builder,
      state: State {
        n_processed: 0,
        latent_batch_decompressors,
        primary_delta_state,
        primary_latents: [T::L::default(); FULL_BATCH_N],
        secondary_latents: [secondary_default; FULL_BATCH_N],
      },
    })
  }

  fn decompress_batch(&mut self, dst: &mut [T]) -> PcoResult<()> {
    let batch_n = dst.len();
    let n = self.n;
    let mode = self.mode;
    let State {
      latent_batch_decompressors,
      primary_delta_state,
      primary_latents,
      secondary_latents,
      n_processed,
      ..
    } = &mut self.state;

    let secondary_latents = &mut secondary_latents[..batch_n];
    let n_latents = latent_batch_decompressors.len();

    self.reader_builder.with_reader(|reader| {
      let primary_dst = if T::TRANSMUTABLE_TO_LATENT {
        T::transmute_to_latents(dst)
      } else {
        &mut primary_latents[..batch_n]
      };
      unsafe {
        decompress_latents_w_delta(
          reader,
          primary_delta_state,
          &mut latent_batch_decompressors[0],
          primary_dst,
          n - *n_processed,
        )
      }
    })?;

    if n_latents >= 2 && self.maybe_constant_secondary.is_none() {
      self.reader_builder.with_reader(|reader| unsafe {
        latent_batch_decompressors[1].decompress_latent_batch(reader, secondary_latents)
      })?;
    }

    if T::TRANSMUTABLE_TO_LATENT {
      T::join_latents(
        mode,
        T::transmute_to_latents(dst),
        secondary_latents,
      );
      convert_from_latents_transmutable(dst);
    } else {
      let primary = &mut primary_latents[..batch_n];
      T::join_latents(mode, primary, secondary_latents);
      convert_from_latents_nontransmutable(primary, dst);
    }

    *n_processed += batch_n;
    if *n_processed == n {
      self.reader_builder.with_reader(|reader| {
        reader.drain_empty_byte("expected trailing bits at end of page to be empty")
      })?;
    }

    Ok(())
  }

  /// Reads the next decompressed numbers into the destination, returning
  /// progress into the page and advancing along the compressed data.
  ///
  /// Will return an error if corruptions or insufficient data are found.
  ///
  /// `dst` must have length either a multiple of 256 or be at least the count
  /// of numbers remaining in the page.
  pub fn decompress(&mut self, num_dst: &mut [T]) -> PcoResult<Progress> {
    if num_dst.len() % FULL_BATCH_N != 0 && num_dst.len() < self.n_remaining() {
      return Err(PcoError::invalid_argument(format!(
        "num_dst's length must either be a multiple of {} or be \
         at least the count of numbers remaining ({} < {})",
        FULL_BATCH_N,
        num_dst.len(),
        self.n_remaining(),
      )));
    }

    let n_to_process = min(num_dst.len(), self.n_remaining());

    let mut n_processed = 0;
    while n_processed < n_to_process {
      let dst_batch_end = min(n_processed + FULL_BATCH_N, n_to_process);
      self.decompress_batch(&mut num_dst[n_processed..dst_batch_end])?;
      n_processed = dst_batch_end;
    }

    Ok(Progress {
      n_processed,
      finished: self.n_remaining() == 0,
    })
  }

  fn n_remaining(&self) -> usize {
    self.n - self.state.n_processed
  }

  /// Returns the rest of the compressed data source.
  pub fn into_src(self) -> R {
    self.reader_builder.into_inner()
  }
}
