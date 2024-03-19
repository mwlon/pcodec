use std::cmp::min;
use std::fmt::Debug;
use std::marker::PhantomData;

use better_io::BetterBufRead;

use crate::bit_reader::{BitReader, BitReaderBuilder};
use crate::constants::{FULL_BATCH_N, PAGE_PADDING};
use crate::data_types::{Latent, NumberLike};
use crate::delta;
use crate::delta::DeltaMoments;
use crate::errors::{PcoError, PcoResult};
use crate::latent_batch_decompressor::LatentBatchDecompressor;
use crate::page_meta::PageMeta;
use crate::progress::Progress;
use crate::{bit_reader, ChunkMeta, Mode};

const PERFORMANT_BUF_READ_CAPACITY: usize = 8192;

#[derive(Clone, Debug)]
pub struct State<L: Latent> {
  n_processed: usize,
  latent_batch_decompressors: Vec<LatentBatchDecompressor<L>>,
  delta_momentss: Vec<DeltaMoments<L>>, // one per latent variable
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
  delta_moments: &mut DeltaMoments<L>,
  lbd: &mut LatentBatchDecompressor<L>,
  dst: &mut [L],
  n_remaining: usize,
) -> PcoResult<()> {
  let n_remaining_pre_delta = n_remaining.saturating_sub(delta_moments.order());
  let pre_delta_len = if dst.len() <= n_remaining_pre_delta {
    dst.len()
  } else {
    // If we're at the end, LatentBatchdDecompressor won't initialize the last
    // few elements before delta decoding them, so we do that manually here to
    // satisfy MIRI. This step isn't really necessary.
    dst[n_remaining_pre_delta..].fill(L::default());
    n_remaining_pre_delta
  };
  lbd.decompress_latent_batch(reader, &mut dst[..pre_delta_len])?;
  delta::decode_in_place(delta_moments, dst);
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
    let delta_momentss = page_meta
      .per_var
      .iter()
      .map(|latent| latent.delta_moments.clone())
      .collect::<Vec<_>>();

    let mut latent_batch_decompressors = Vec::new();
    for latent_idx in 0..mode.n_latent_vars() {
      let chunk_latent_meta = &chunk_meta.per_latent_var[latent_idx];
      if chunk_latent_meta.bins.is_empty() && n > chunk_meta.delta_encoding_order {
        return Err(PcoError::corruption(format!(
          "unable to decompress chunk with no bins and {} deltas",
          n - chunk_meta.delta_encoding_order,
        )));
      }

      latent_batch_decompressors.push(LatentBatchDecompressor::new(
        chunk_latent_meta,
        &page_meta.per_var[latent_idx],
      )?);
    }

    let maybe_constant_secondary =
      if latent_batch_decompressors.len() >= 2 && delta_momentss[1].order() == 0 {
        latent_batch_decompressors[1].maybe_constant_value
      } else {
        None
      };

    // we don't store the whole ChunkMeta because it can get large due to bins
    let secondary_default = maybe_constant_secondary.unwrap_or(T::L::default());
    Ok(Self {
      n,
      mode,
      maybe_constant_secondary,
      phantom: PhantomData,
      reader_builder,
      state: State {
        n_processed: 0,
        latent_batch_decompressors,
        delta_momentss,
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
      delta_momentss,
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
          &mut delta_momentss[0],
          &mut latent_batch_decompressors[0],
          primary_dst,
          n - *n_processed,
        )
      }
    })?;

    if n_latents >= 2 && self.maybe_constant_secondary.is_none() {
      self.reader_builder.with_reader(|reader| unsafe {
        decompress_latents_w_delta(
          reader,
          &mut delta_momentss[1],
          &mut latent_batch_decompressors[1],
          secondary_latents,
          n - *n_processed,
        )
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
