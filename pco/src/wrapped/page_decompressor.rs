use std::cmp::min;
use std::fmt::Debug;
use std::marker::PhantomData;

use better_io::BetterBufRead;

use crate::bit_reader;
use crate::bit_reader::{BitReader, BitReaderBuilder};
use crate::constants::{FULL_BATCH_N, PAGE_PADDING};
use crate::data_types::{Latent, Number};
use crate::delta;
use crate::delta::DeltaMoments;
use crate::errors::{PcoError, PcoResult};
use crate::latent_batch_decompressor::LatentBatchDecompressor;
use crate::metadata::page::PageMeta;
use crate::metadata::{ChunkMeta, DeltaEncoding, Mode};
use crate::progress::Progress;

const PERFORMANT_BUF_READ_CAPACITY: usize = 8192;

#[derive(Clone, Debug)]
pub struct State<L: Latent> {
  n_processed: usize,
  latent_batch_decompressors: Vec<LatentBatchDecompressor<L>>,
  delta_momentss: Vec<DeltaMoments<L>>, // one per latent variable
  secondary_latents: [L; FULL_BATCH_N],
}

/// Holds metadata about a page and supports decompression.
pub struct PageDecompressor<T: Number, R: BetterBufRead> {
  // immutable
  n: usize,
  mode: Mode,
  delta_encoding: DeltaEncoding,
  maybe_constant_latents: Vec<Option<T::L>>, // 1 per latent var
  phantom: PhantomData<T>,

  // mutable
  reader_builder: BitReaderBuilder<R>,
  state: State<T::L>,
}

unsafe fn decompress_latents_w_delta<L: Latent>(
  reader: &mut BitReader,
  delta_encoding: DeltaEncoding,
  n_remaining: usize,
  delta_state: &mut DeltaMoments<L>,
  lbd: &mut LatentBatchDecompressor<L>,
  dst: &mut [L],
) -> PcoResult<()> {
  let n_remaining_pre_delta = n_remaining.saturating_sub(delta_state.order());
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
  match delta_encoding {
    DeltaEncoding::None => (),
    DeltaEncoding::Consecutive(_) => delta::decode_in_place(delta_state, dst),
  }
  Ok(())
}

fn convert_from_latents_to_numbers<T: Number>(dst: &mut [T]) {
  // we wrote the joined latents to dst, so we can convert them in place
  for l_and_dst in dst {
    *l_and_dst = T::from_latent_ordered(l_and_dst.transmute_to_latent());
  }
}

impl<T: Number, R: BetterBufRead> PageDecompressor<T, R> {
  pub(crate) fn new(mut src: R, chunk_meta: &ChunkMeta, n: usize) -> PcoResult<Self> {
    bit_reader::ensure_buf_read_capacity(&mut src, PERFORMANT_BUF_READ_CAPACITY);
    let mut reader_builder = BitReaderBuilder::new(src, PAGE_PADDING, 0);

    let page_meta = reader_builder
      .with_reader(|reader| unsafe { PageMeta::read_from::<T::L>(reader, chunk_meta) })?;

    let mode = chunk_meta.mode;
    let delta_momentss = page_meta
      .per_latent_var
      .iter()
      .map(|latent_var_meta| {
        let moments = latent_var_meta
          .delta_moments
          .downcast_ref::<T::L>()
          .unwrap()
          .clone();
        DeltaMoments(moments)
      })
      .collect::<Vec<_>>();

    let mut latent_batch_decompressors = Vec::new();
    for latent_idx in 0..mode.n_latent_vars() {
      let chunk_latent_meta = &chunk_meta.per_latent_var[latent_idx];

      // this will change to dynamically typed soon
      let bins = chunk_latent_meta.bins.downcast_ref::<T::L>().unwrap();
      let n_in_body = n.saturating_sub(chunk_meta.delta_encoding.n_latents_per_state());
      if bins.is_empty() && n_in_body > 0 {
        return Err(PcoError::corruption(format!(
          "unable to decompress chunk with no bins and {} latents",
          n_in_body
        )));
      }

      latent_batch_decompressors.push(LatentBatchDecompressor::new(
        chunk_latent_meta.ans_size_log,
        bins,
        page_meta.per_latent_var[latent_idx].ans_final_state_idxs,
      )?);
    }

    let maybe_constant_secondary =
      if latent_batch_decompressors.len() >= 2 && delta_momentss[1].order() == 0 {
        latent_batch_decompressors[1].maybe_constant_value
      } else {
        None
      };
    let maybe_constant_latents = vec![None, maybe_constant_secondary];

    // we don't store the whole ChunkMeta because it can get large due to bins
    let secondary_default = maybe_constant_secondary.unwrap_or(T::L::default());
    Ok(Self {
      n,
      mode,
      delta_encoding: chunk_meta.delta_encoding,
      maybe_constant_latents,
      phantom: PhantomData,
      reader_builder,
      state: State {
        n_processed: 0,
        latent_batch_decompressors,
        delta_momentss,
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
      secondary_latents,
      n_processed,
      ..
    } = &mut self.state;

    let secondary_latents = &mut secondary_latents[..batch_n];
    let n_latents = latent_batch_decompressors.len();

    self.reader_builder.with_reader(|reader| {
      let primary_dst = T::transmute_to_latents(dst);
      unsafe {
        decompress_latents_w_delta(
          reader,
          mode.delta_encoding_for_latent_var(0, self.delta_encoding),
          n - *n_processed,
          &mut delta_momentss[0],
          &mut latent_batch_decompressors[0],
          primary_dst,
        )
      }
    })?;

    if n_latents >= 2 && self.maybe_constant_latents[1].is_none() {
      self.reader_builder.with_reader(|reader| unsafe {
        decompress_latents_w_delta(
          reader,
          mode.delta_encoding_for_latent_var(1, self.delta_encoding),
          n - *n_processed,
          &mut delta_momentss[1],
          &mut latent_batch_decompressors[1],
          secondary_latents,
        )
      })?;
    }

    T::join_latents(
      mode,
      T::transmute_to_latents(dst),
      secondary_latents,
    );
    convert_from_latents_to_numbers(dst);

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
