use std::cmp::min;
use std::marker::PhantomData;
use better_io::{BetterBufRead};

use crate::bit_reader::{BitReader, BitReaderBuilder};
use crate::constants::{Bitlen, FULL_BATCH_N, PAGE_PADDING};
use crate::data_types::{NumberLike, UnsignedLike};
use crate::delta::DeltaMoments;
use crate::errors::{PcoError, PcoResult};
use crate::latent_batch_decompressor::LatentBatchDecompressor;
use crate::page_meta::PageMeta;
use crate::progress::Progress;
use crate::wrapped::chunk_decompressor::ChunkDecompressor;
use crate::{bit_reader, delta, float_mult_utils};
use crate::{latent_batch_decompressor, Mode};

#[derive(Clone, Debug)]
pub struct State<U: UnsignedLike> {
  n_processed: usize,
  latent_batch_decompressors: Vec<LatentBatchDecompressor<U>>,
  delta_momentss: Vec<DeltaMoments<U>>, // one per latent variable
  secondary_latents: [U; FULL_BATCH_N],
  bits_past_byte: Bitlen, // in [0, 8), only used to start a batch
}

/// Holds metadata about a page and supports decompression.
#[derive(Clone, Debug)]
pub struct PageDecompressor<T: NumberLike> {
  // immutable
  n: usize,
  mode: Mode<T::Unsigned>,
  phantom: PhantomData<T>,

  // mutable
  state: State<T::Unsigned>,
}

#[inline(never)]
fn unsigneds_to_nums_in_place<T: NumberLike>(dst: &mut [T::Unsigned]) {
  for u in dst.iter_mut() {
    *u = T::transmute_to_unsigned(T::from_unsigned(*u));
  }
}

fn join_latents<U: UnsignedLike>(mode: Mode<U>, primary: &mut [U], secondary: &mut [U]) {
  // For classic and GCD modes, we already wrote the unsigneds into the primary
  // latent stream directly.
  if let Mode::FloatMult(config) = mode {
    float_mult_utils::join_latents(config.base, primary, secondary);
  }
}

fn decompress_latents_w_delta<U: UnsignedLike>(
  reader: &mut BitReader,
  delta_moments: &mut DeltaMoments<U>,
  lbd: &mut LatentBatchDecompressor<U>,
  dst: &mut [U],
  n_remaining: usize,
) -> PcoResult<()> {
  let pre_delta_len = min(
    dst.len(),
    n_remaining.saturating_sub(delta_moments.order()),
  );
  lbd.decompress_latent_batch_dirty(reader, &mut dst[..pre_delta_len])?;
  delta::decode_in_place(delta_moments, &mut dst[..]);
  Ok(())
}

impl<T: NumberLike> PageDecompressor<T> {
  pub(crate) fn new(
    chunk_decompressor: &ChunkDecompressor<T>,
    n: usize,
    page_meta: PageMeta<T::Unsigned>,
    bits_past_byte: Bitlen,
  ) -> PcoResult<Self> {
    let chunk_meta = &chunk_decompressor.meta;
    let mode = chunk_meta.mode;
    let delta_momentss = page_meta
      .per_latent_var
      .iter()
      .map(|latent| latent.delta_moments.clone())
      .collect();

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
        &page_meta.per_latent_var[latent_idx],
        chunk_meta.mode,
      )?);
    }
    // we don't store the whole ChunkMeta because it can get large due to bins
    Ok(Self {
      n,
      mode,
      phantom: PhantomData,
      state: State {
        n_processed: 0,
        latent_batch_decompressors,
        delta_momentss,
        secondary_latents: [T::Unsigned::default(); FULL_BATCH_N],
        bits_past_byte,
      },
    })
  }

  fn decompress_batch<R: BetterBufRead>(
    &mut self,
    reader_builder: &mut BitReaderBuilder<R>,
    primary_dst: &mut [T],
  ) -> PcoResult<()> {
    let batch_n = primary_dst.len();
    let primary_latents = T::transmute_to_unsigned_slice(primary_dst);
    let n = self.n;
    let mode = self.mode;
    let State {
      latent_batch_decompressors,
      delta_momentss,
      secondary_latents,
      n_processed,
      ..
    } = &mut self.state;

    let secondary_latents = &mut secondary_latents[..batch_n];
    let n_latents = latent_batch_decompressors.len();

    if n_latents >= 1 {
      reader_builder.with_reader(|reader| {
        decompress_latents_w_delta(
          reader,
          &mut delta_momentss[0],
          &mut latent_batch_decompressors[0],
          primary_latents,
          n - *n_processed,
        )
      })?;
    }
    if n_latents >= 2 {
      reader_builder.with_reader(|reader| {
        decompress_latents_w_delta(
          reader,
          &mut delta_momentss[1],
          &mut latent_batch_decompressors[1],
          secondary_latents,
          n - *n_processed,
        )
      })?;
    }

    join_latents(mode, primary_latents, secondary_latents);
    unsigneds_to_nums_in_place::<T>(primary_latents);

    *n_processed += batch_n;
    if *n_processed == n {
      reader_builder.with_reader(|reader| {
        reader.drain_empty_byte("expected trailing bits at end of page to be empty")
      })?;
    }

    Ok(())
  }

  /// Reads compressed numbers into the destination, returning progress and
  /// the number of bytes read.
  ///
  /// Will return an error if corruptions or insufficient data are found.
  ///
  /// `dst` must have length either a multiple of 256 or be at least the count
  /// of numbers remaining in the page.
  pub fn decompress<R: BetterBufRead>(&mut self, mut src: R, num_dst: &mut [T]) -> PcoResult<(Progress, R)> {
    if num_dst.len() % FULL_BATCH_N != 0 && num_dst.len() < self.n_remaining() {
      return Err(PcoError::invalid_argument(format!(
        "num_dst's length must either be a multiple of {} or be \
         at least the count of numbers remaining ({} < {})",
        FULL_BATCH_N,
        num_dst.len(),
        self.n_remaining(),
      )));
    }

    src.resize_capacity(8192); // TODO
    let mut reader_builder = BitReaderBuilder::new(src, PAGE_PADDING, self.state.bits_past_byte);

    let n_to_process = min(num_dst.len(), self.n_remaining());

    let mut n_processed = 0;
    while n_processed < n_to_process {
      let dst_batch_end = min(n_processed + FULL_BATCH_N, n_to_process);
      self.decompress_batch(
        &mut reader_builder,
        &mut num_dst[n_processed..dst_batch_end],
      )?;
      n_processed = dst_batch_end;
    }

    let progress = Progress {
      n_processed,
      finished_page: self.n_remaining() == 0,
    };
    self.state.bits_past_byte = reader_builder.bits_past_byte();

    Ok((progress, reader_builder.into_inner()))
  }

  fn n_remaining(&self) -> usize {
    self.n - self.state.n_processed
  }
}
