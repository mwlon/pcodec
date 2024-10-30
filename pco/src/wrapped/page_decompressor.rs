use std::cmp::min;
use std::fmt::Debug;
use std::marker::PhantomData;

use better_io::BetterBufRead;

use crate::bit_reader::{BitReader, BitReaderBuilder};
use crate::constants::{DeltaLookback, FULL_BATCH_N, PAGE_PADDING};
use crate::data_types::{Latent, Number};
use crate::delta::DeltaState;
use crate::errors::{PcoError, PcoResult};
use crate::latent_batch_decompressor::LatentBatchDecompressor;
use crate::metadata::page::PageMeta;
use crate::metadata::{ChunkLatentVarMeta, ChunkMeta, DeltaEncoding, DynBins, DynLatent, DynLatents, Mode};
use crate::per_latent_var::{LatentVarKey, PerLatentVar, PerLatentVarBuilder};
use crate::progress::Progress;
use crate::{bit_reader, define_latent_enum};
use crate::{delta, match_latent_enum};

const PERFORMANT_BUF_READ_CAPACITY: usize = 8192;

struct LatentScratch {
  is_constant: bool,
  dst: DynLatents,
}

struct LatentPageDecompressor<L: Latent> {
  delta_encoding: DeltaEncoding,
  latent_batch_decompressor: LatentBatchDecompressor<L>,
  delta_state: Vec<L>,
}

impl<L: Latent> LatentPageDecompressor<L> {
  unsafe fn decompress_latents_w_delta<L: Latent>(
    &mut self,
    reader: &mut BitReader,
    delta_latents: Option<&DynLatents>,
    n_remaining: usize,
    dst: &mut [L],
  ) -> PcoResult<()> {
    let n_remaining_pre_delta = n_remaining.saturating_sub(self.delta_encoding.n_latents_per_state());
    let pre_delta_len = if dst.len() <= n_remaining_pre_delta {
      dst.len()
    } else {
      // If we're at the end, LatentBatchdDecompressor won't initialize the last
      // few elements before delta decoding them, so we do that manually here to
      // satisfy MIRI. This step isn't really necessary.
      dst[n_remaining_pre_delta..].fill(L::default());
      n_remaining_pre_delta
    };
    self.latent_batch_decompressor.decompress_latent_batch(reader, &mut dst[..pre_delta_len])?;
    match self.delta_encoding {
      DeltaEncoding::None => (),
      DeltaEncoding::Consecutive(_) => delta::decode_consecutive_in_place(&mut self.delta_state, dst),
      DeltaEncoding::Lz77(config) => delta::decode_lz77_in_place(
        config,
        delta_latents.unwrap().downcast_ref::<DeltaLookback>().unwrap(),
        &mut self.delta_state,
        dst,
      ),
    }
    Ok(())
  }
}

define_latent_enum!(
  #[derive()]
  DynLatentPageDecompressor(LatentPageDecompressor)
);

/// Holds metadata about a page and supports decompression.
pub struct PageDecompressor<T: Number, R: BetterBufRead> {
  // immutable
  n: usize,
  mode: Mode,
  delta_encoding: DeltaEncoding,
  phantom: PhantomData<T>,

  // mutable
  reader_builder: BitReaderBuilder<R>,
  n_processed: usize,
  latent_decompressors: PerLatentVar<DynLatentPageDecompressor>,
  delta_scratch: LatentScratch,
  secondary_scratch: LatentScratch,
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

    let mut states = PerLatentVarBuilder::default();
    for (key, (chunk_latent_var_meta, page_latent_var_meta)) in chunk_meta
      .per_latent_var
      .as_ref()
      .zip_exact(page_meta.per_latent_var.as_ref())
      .enumerated()
    {
      let state = match_latent_enum!(
        &chunk_latent_var_meta.bins,
        DynBins<L>(bins) => {
          let delta_state = page_latent_var_meta
            .delta_moments
            .downcast_ref::<L>()
            .unwrap()
            .clone();

          let var_delta_encoding = chunk_meta.delta_encoding.for_latent_var(key);
          let n_in_body = n.saturating_sub(var_delta_encoding.n_latents_per_state());
          if bins.is_empty() && n_in_body > 0 {
            return Err(PcoError::corruption(format!(
              "unable to decompress chunk with no bins and {} latents",
              n_in_body
            )));
          }

          let latent_batch_decompressor = LatentBatchDecompressor::new(
            chunk_latent_var_meta.ans_size_log,
            bins,
            page_latent_var_meta.ans_final_state_idxs,
          )?;

          DynLatentPageDecompressor::new(LatentPageDecompressor {
            delta_state,
            latent_batch_decompressor
            delta_encoding: var_delta_encoding,
          }).unwrap()
        }
      );

      states.set(key, state);
    }
    let latent_decompressors: PerLatentVar<DynLatentPageDecompressor> = states.into();

    fn make_latent_scratch(lpd: Option<&DynLatentPageDecompressor>) -> LatentScratch {
      let Some(lpd) = lpd else {
        return LatentScratch {
          is_constant: true,
          dst: DynLatents::new(Vec::<u64>::new()).unwrap(),
        }
      };

      match_latent_enum!(
        lpd,
        DynLatentPageDecompressor<L>(inner) => {
          let maybe_constant_value = inner.latent_batch_decompressor.maybe_constant_value;
          LatentScratch {
            is_constant: maybe_constant_value.is_some(),
            dst: DynLatents::new(vec![maybe_constant_value.unwrap_or_default(); FULL_BATCH_N]).unwrap(),
          }
        }
      )
    }
    let delta_scratch = make_latent_scratch(latent_decompressors.delta.as_ref());
    let secondary_scratch = make_latent_scratch(latent_decompressors.secondary.as_ref());

    // we don't store the whole ChunkMeta because it can get large due to bins
    Ok(Self {
      n,
      mode,
      delta_encoding: chunk_meta.delta_encoding,
      phantom: PhantomData,
      reader_builder,
      n_processed: 0,
      latent_decompressors,
      delta_scratch,
      secondary_scratch,
    })
  }

  fn decompress_batch(&mut self, dst: &mut [T]) -> PcoResult<()> {
    let batch_n = dst.len();
    let n = self.n;
    let mode = self.mode;

    if let Some(delta_state) = &mut self.latent_decompressors.delta {
      self.reader_builder.with_reader(|reader| {
        match_latent_enum!(
          delta_state,
          DynLatentPageDecompressor<L>(lpd) => {
            unsafe {
              // We never apply delta encoding to delta latents, so we just
              // skip straight to the inner LatentBatchDecompressor
              lpd.latent_batch_decompressor.decompress_latent_batch(
                reader,
                &mut self.delta_scratch.dst.downcast_mut::<L>().unwrap()[..batch_n]
              )
            }
          }
        )
      })?;
    }

    self.reader_builder.with_reader(|reader| {
      let primary_dst = T::transmute_to_latents(dst);
      let state = &mut self.latent_decompressors.primary;
      unsafe {
        decompress_latents_w_delta(
          reader,
          self.delta_encoding.for_latent_var(LatentVarKey::Primary),
          n - self.n_processed,
          &mut state.,
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
          n - self.n_processed,
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

    self.n_processed += batch_n;
    if self.n_processed == n {
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
