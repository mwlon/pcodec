use std::cmp::min;
use std::marker::PhantomData;

use crate::latent_batch_decompressor::{LatentBatchDecompressor};
use crate::bit_reader::BitReader;
use crate::chunk_metadata::PageMetadata;
use crate::constants::{FULL_BATCH_SIZE};
use crate::data_types::{NumberLike, UnsignedLike};
use crate::delta::DeltaMoments;
use crate::errors::{PcoError, PcoResult};
use crate::progress::Progress;
use crate::{latent_batch_decompressor, Mode};
use crate::{delta, float_mult_utils, ChunkMetadata};

#[derive(Clone, Debug)]
pub struct State<U: UnsignedLike> {
  n_processed: usize,
  n_bits_processed: usize,
  latent_batch_decompressors: Vec<LatentBatchDecompressor<U>>,
  delta_momentss: Vec<DeltaMoments<U>>, // one per latent variable
  // Secondary latents is technically mutable, but it doesn't really matter
  // since we overwrite it on every call.
  secondary_latents: [U; FULL_BATCH_SIZE],
}

pub struct Backup<U: UnsignedLike> {
  n_processed: usize,
  n_bits_processed: usize,
  latent_batch_backups: Vec<latent_batch_decompressor::Backup>,
  delta_momentss: Vec<DeltaMoments<U>>,
}

impl<U: UnsignedLike> State<U> {
  fn backup(&self) -> Backup<U> {
    Backup {
      n_processed: self.n_processed,
      n_bits_processed: self.n_bits_processed,
      latent_batch_backups: self.latent_batch_decompressors.iter().map(|lbd| lbd.backup()).collect::<Vec<_>>(),
      delta_momentss: self.delta_momentss.clone(),
    }
  }

  fn recover(&mut self, backup: Backup<U>) {
    self.n_processed = backup.n_processed;
    self.n_bits_processed = backup.n_bits_processed;
    self.latent_batch_decompressors.iter_mut().zip(backup.latent_batch_backups.into_iter()).for_each(|(lbd, lbd_backup)| {
      lbd.recover(lbd_backup);
    });
    self.delta_momentss = backup.delta_momentss;
  }
}

// PageDecompressor wraps BatchDecompressor and handles reconstruction from
// delta encoding.
#[derive(Clone, Debug)]
pub struct PageDecompressor<T: NumberLike> {
  // immutable
  n: usize,
  n_bits: usize,
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
    n: usize,
    compressed_body_size: usize,
    chunk_meta: &ChunkMetadata<T::Unsigned>,
    page_meta: PageMetadata<T::Unsigned>,
  ) -> PcoResult<Self> {
    let mode = chunk_meta.mode;
    let delta_momentss = page_meta
      .latents
      .iter()
      .map(|latent| latent.delta_moments.clone())
      .collect();

    let (needs_gcd, n_nontrivial_latents) = chunk_meta.nontrivial_gcd_and_n_latents();
    let mut latent_batch_decompressors = Vec::new();
    for latent_idx in 0..mode.n_latents() {
      if chunk_meta.latents[latent_idx].bins.is_empty() && n > chunk_meta.delta_encoding_order {
        return Err(PcoError::corruption(format!(
          "unable to decompress chunk with no bins and {} deltas",
          n - chunk_meta.delta_encoding_order,
        )));
      }

      let is_trivial = latent_idx >= n_nontrivial_latents;
      latent_batch_decompressors.push(LatentBatchDecompressor::new(
        &chunk_meta.latents[latent_idx],
        &page_meta.latents[latent_idx],
        needs_gcd,
        is_trivial,
      )?);
    }
    // we don't store the whole ChunkMeta because it can get large due to bins
    Ok(Self {
      n,
      n_bits: compressed_body_size * 8,
      mode,
      phantom: PhantomData,
      state: State {
        n_processed: 0,
        n_bits_processed: 0,
        latent_batch_decompressors,
        delta_momentss,
        secondary_latents: [T::Unsigned::default(); FULL_BATCH_SIZE],
      },
    })
  }

  // dirties reader and state, but might fail midway
  fn decompress_batch_dirty(
    &mut self,
    reader: &mut BitReader,
    primary_dst: &mut [T],
  ) -> PcoResult<()> {
    let batch_size = primary_dst.len();
    let initial_bit_idx = reader.bit_idx();
    let primary_latents = T::transmute_to_unsigned_slice(primary_dst);
    let n = self.n;
    let n_bits = self.n_bits;
    let mode = self.mode;
    let State {
      latent_batch_decompressors,
      delta_momentss,
      secondary_latents,
      n_processed,
      n_bits_processed,
      ..
    } = &mut self.state;

    let secondary_latents = &mut secondary_latents[..batch_size];
    let n_latents = latent_batch_decompressors.len();

    if n_latents >= 1 {
      decompress_latents_w_delta(reader, &mut delta_momentss[0], &mut latent_batch_decompressors[0], primary_latents, n - *n_processed)?;
    }
    if n_latents >= 2 {
      decompress_latents_w_delta(reader, &mut delta_momentss[1], &mut latent_batch_decompressors[1], secondary_latents, n - *n_processed)?;
    }

    join_latents(mode, primary_latents, secondary_latents);
    unsigneds_to_nums_in_place::<T>(primary_latents);

    *n_processed += batch_size;
    if *n_processed == n {
      reader.drain_empty_byte("expected trailing bits at end of page to be empty")?;
    }
    *n_bits_processed += reader.bit_idx() - initial_bit_idx;

    if *n_processed >= n && *n_bits_processed != n_bits {
      return Err(PcoError::corruption(format!(
        "Expected {} bits in data page but read {} by the end",
        n_bits,
        *n_bits_processed,
      )));
    } else if *n_bits_processed > n_bits {
      return Err(PcoError::corruption(format!(
        "Expected {} bits in data page but read {} before reaching the end",
        n_bits,
        *n_bits_processed,
      )));
    }

    Ok(())
  }

  // If this returns an error, this and reader will be unchanged, but num_dst
  // might be modified.
  pub fn decompress(
    &mut self,
    reader: &mut BitReader,
    num_dst: &mut [T],
  ) -> PcoResult<Progress> {
    if num_dst.len() % FULL_BATCH_SIZE != 0 && num_dst.len() < self.n_remaining() {
      return Err(PcoError::invalid_argument(format!(
        "num_dst's length must either be a multiple of {} or exceed the length of numbers remaining ({}) (was {})",
        FULL_BATCH_SIZE,
        self.n_remaining(),
        num_dst.len(),
      )));
    }
    let n_to_process = min(num_dst.len(), self.n_remaining());
    let initial_bit_idx = reader.bit_idx();
    let backup = self.state.backup();

    let mut n_processed = 0;
    while n_processed < n_to_process {
      let dst_batch_end = min(n_processed + FULL_BATCH_SIZE, n_to_process);
      let batch_res = self.decompress_batch_dirty(
        reader,
        &mut num_dst[n_processed..dst_batch_end],
      );

      if let Err(e) = batch_res {
        self.state.recover(backup);
        reader.seek_to(initial_bit_idx);
        return Err(e);
      }

      n_processed = dst_batch_end;
    }

    Ok(Progress {
      n_processed,
      finished_page: self.n_remaining() == 0,
    })
  }

  pub fn bits_remaining(&self) -> usize {
    self.n_bits - self.state.n_bits_processed
  }

  pub fn n_remaining(&self) -> usize {
    self.n - self.state.n_processed
  }
}
