use std::cmp::min;
use std::fmt::Debug;
use std::marker::PhantomData;

use better_io::BetterBufRead;

use crate::bit_reader;
use crate::bit_reader::BitReaderBuilder;
use crate::constants::{FULL_BATCH_N, PAGE_PADDING};
use crate::data_types::{Latent, Number};
use crate::errors::{PcoError, PcoResult};
use crate::latent_page_decompressor::LatentPageDecompressor;
use crate::macros::{define_latent_enum, match_latent_enum};
use crate::metadata::page::PageMeta;
use crate::metadata::{ChunkMeta, DeltaEncoding, DynBins, DynLatents, Mode};
use crate::per_latent_var::{PerLatentVar, PerLatentVarBuilder};
use crate::progress::Progress;

const PERFORMANT_BUF_READ_CAPACITY: usize = 8192;

struct LatentScratch {
  is_constant: bool,
  dst: DynLatents,
}

// struct LatentPageDecompressor<L: Latent> {
//   delta_encoding: DeltaEncoding,
//   latent_batch_decompressor: LatentPageDecompressor<L>,
//   delta_state: Vec<L>,
// }
//
// impl<L: Latent> LatentPageDecompressor<L> {
//   unsafe fn decompress_latents_w_delta<L: Latent>(
//     &mut self,
//     reader: &mut BitReader,
//     delta_latents: Option<&DynLatents>,
//     n_remaining: usize,
//     dst: &mut [L],
//   ) -> PcoResult<()> {
//     let n_remaining_pre_delta = n_remaining.saturating_sub(self.delta_encoding.n_latents_per_state());
//     let pre_delta_len = if dst.len() <= n_remaining_pre_delta {
//       dst.len()
//     } else {
//       // If we're at the end, LatentBatchdDecompressor won't initialize the last
//       // few elements before delta decoding them, so we do that manually here to
//       // satisfy MIRI. This step isn't really necessary.
//       dst[n_remaining_pre_delta..].fill(L::default());
//       n_remaining_pre_delta
//     };
//     self.latent_batch_decompressor.decompress_latent_batch(reader, &mut dst[..pre_delta_len])?;
//     match self.delta_encoding {
//       DeltaEncoding::None => (),
//       DeltaEncoding::Consecutive(_) => delta::decode_consecutive_in_place(&mut self.delta_state, dst),
//       DeltaEncoding::Lz77(config) => delta::decode_lz77_in_place(
//         config,
//         delta_latents.unwrap().downcast_ref::<DeltaLookback>().unwrap(),
//         &mut self.delta_state,
//         dst,
//       ),
//     }
//     Ok(())
//   }
// }

define_latent_enum!(
  #[derive()]
  DynLatentPageDecompressor(LatentPageDecompressor)
);

/// Holds metadata about a page and supports decompression.
pub struct PageDecompressor<T: Number, R: BetterBufRead> {
  // immutable
  n: usize,
  mode: Mode,
  phantom: PhantomData<T>,

  // mutable
  reader_builder: BitReaderBuilder<R>,
  n_processed: usize,
  // TODO make these heap allocated
  latent_decompressors: PerLatentVar<DynLatentPageDecompressor>,
  delta_scratch: Option<LatentScratch>,
  secondary_scratch: Option<LatentScratch>,
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

          let lpd= LatentPageDecompressor::new(
            chunk_latent_var_meta.ans_size_log,
            bins,
            var_delta_encoding,
            page_latent_var_meta.ans_final_state_idxs,
            delta_state,
          )?;

          DynLatentPageDecompressor::new(lpd).unwrap()
        }
      );

      states.set(key, state);
    }
    let latent_decompressors: PerLatentVar<DynLatentPageDecompressor> = states.into();

    fn make_latent_scratch(lpd: Option<&DynLatentPageDecompressor>) -> Option<LatentScratch> {
      let Some(lpd) = lpd else {
        return None;
      };

      match_latent_enum!(
        lpd,
        DynLatentPageDecompressor<L>(inner) => {
          let maybe_constant_value = inner.maybe_constant_value;
          Some(LatentScratch {
            is_constant: maybe_constant_value.is_some(),
            dst: DynLatents::new(vec![maybe_constant_value.unwrap_or_default(); FULL_BATCH_N]).unwrap(),
          })
        }
      )
    }
    let delta_scratch = make_latent_scratch(latent_decompressors.delta.as_ref());
    let secondary_scratch = make_latent_scratch(latent_decompressors.secondary.as_ref());

    // we don't store the whole ChunkMeta because it can get large due to bins
    Ok(Self {
      n,
      mode,
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
    let n_remaining = self.n_remaining();
    let mode = self.mode;

    // DELTA LATENTS
    if let Some(LatentScratch {
      is_constant: false,
      dst,
    }) = &mut self.delta_scratch
    {
      let dyn_lpd = self.latent_decompressors.delta.as_mut().unwrap();
      self.reader_builder.with_reader(|reader| unsafe {
        match_latent_enum!(
          dyn_lpd,
          DynLatentPageDecompressor<L>(lpd) => {
            // We never apply delta encoding to delta latents, so we just
            // skip straight to the inner LatentBatchDecompressor
            lpd.decompress_batch_pre_delta(
              reader,
              &mut dst.downcast_mut::<L>().unwrap()[..batch_n]
            )
          }
        );
        Ok(())
      })?;
    }
    let delta_latents = self.delta_scratch.as_ref().map(|scratch| &scratch.dst);

    // PRIMARY LATENTS
    // TODO should primary latents always be L or be flexible?
    self.reader_builder.with_reader(|reader| unsafe {
      let primary_dst = T::transmute_to_latents(dst);
      let dyn_lpd = self
        .latent_decompressors
        .primary
        .downcast_mut::<T::L>()
        .unwrap();
      dyn_lpd.decompress_batch(
        delta_latents,
        n_remaining,
        reader,
        primary_dst,
      );
      Ok(())
    })?;

    // SECONDARY LATENTS
    if let Some(LatentScratch {
      is_constant: false,
      dst,
    }) = &mut self.secondary_scratch
    {
      let dyn_lpd = self.latent_decompressors.secondary.as_mut().unwrap();
      self.reader_builder.with_reader(|reader| unsafe {
        match_latent_enum!(
          dyn_lpd,
          DynLatentPageDecompressor<L>(lpd) => {
            // We never apply delta encoding to delta latents, so we just
            // skip straight to the inner LatentBatchDecompressor
            lpd.decompress_batch(
              delta_latents,
              n_remaining,
              reader,
              &mut dst.downcast_mut::<L>().unwrap()[..batch_n]
            )
          }
        );
        Ok(())
      })?;
    }

    T::join_latents(
      mode,
      T::transmute_to_latents(dst),
      self.secondary_scratch.as_ref().map(|scratch| &scratch.dst),
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
    self.n - self.n_processed
  }

  /// Returns the rest of the compressed data source.
  pub fn into_src(self) -> R {
    self.reader_builder.into_inner()
  }
}
