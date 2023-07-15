use std::cmp::min;
use std::marker::PhantomData;

use crate::bit_reader::BitReader;
use crate::chunk_metadata::DataPageMetadata;
use crate::constants::UNSIGNED_BATCH_SIZE;
use crate::data_types::{NumberLike, UnsignedLike};
use crate::delta_encoding::DeltaMoments;
use crate::errors::PcoResult;
use crate::num_decompressor::NumDecompressor;
use crate::progress::Progress;
use crate::unsigned_src_dst::UnsignedDst;
use crate::{delta_encoding, float_mult_utils, ChunkMetadata};
use crate::{num_decompressor, Mode};

// BodyDecompressor wraps NumDecompressor and handles reconstruction from
// delta encoding.
#[derive(Clone, Debug)]
pub struct BodyDecompressor<T: NumberLike> {
  mode: Mode<T::Unsigned>,
  num_decompressor: Box<dyn NumDecompressor<T::Unsigned>>,
  delta_momentss: Vec<DeltaMoments<T::Unsigned>>, // one per stream
  secondary_stream: [T::Unsigned; UNSIGNED_BATCH_SIZE],
  phantom: PhantomData<T>,
}

#[inline(never)]
fn unsigneds_to_nums_in_place<T: NumberLike>(dest: &mut [T::Unsigned]) {
  for u in dest.iter_mut() {
    *u = T::transmute_to_unsigned(T::from_unsigned(*u));
  }
}

fn join_streams<U: UnsignedLike>(mode: Mode<U>, dst: UnsignedDst<U>) {
  // For classic and GCD modes, we already wrote the unsigneds into the primary
  // stream directly.
  if let Mode::FloatMult(config) = mode {
    float_mult_utils::join_streams(config.base, dst);
  }
}

impl<T: NumberLike> BodyDecompressor<T> {
  pub(crate) fn new(
    n: usize,
    compressed_body_size: usize,
    chunk_meta: &ChunkMetadata<T::Unsigned>,
    data_page_meta: DataPageMetadata<T::Unsigned>,
  ) -> PcoResult<Self> {
    let delta_momentss = data_page_meta
      .streams
      .iter()
      .map(|stream| stream.delta_moments.clone())
      .collect();
    let num_decompressor = num_decompressor::new(
      n,
      compressed_body_size,
      chunk_meta,
      data_page_meta,
    )?;
    Ok(Self {
      // we don't store the whole ChunkMeta because it can get large due to bins
      mode: chunk_meta.mode,
      num_decompressor,
      delta_momentss,
      secondary_stream: [T::Unsigned::default(); UNSIGNED_BATCH_SIZE],
      phantom: PhantomData,
    })
  }

  #[inline(never)]
  fn decompress_batch(
    &mut self,
    reader: &mut BitReader,
    error_on_insufficient_data: bool,
    num_dst: &mut [T],
  ) -> PcoResult<Progress> {
    let batch_end = min(UNSIGNED_BATCH_SIZE, num_dst.len());
    let unsigneds_mut = T::transmute_to_unsigned_slice(&mut num_dst[..batch_end]);
    let Self {
      num_decompressor,
      delta_momentss,
      ..
    } = self;
    let n_streams = self.mode.n_streams();

    if let Some(initial_value_required) = num_decompressor.initial_value_required(0) {
      unsigneds_mut.fill(initial_value_required);
    }
    if let Some(initial_value_required) = num_decompressor.initial_value_required(1) {
      self.secondary_stream.fill(initial_value_required);
    }

    let progress = {
      let mut u_dst = UnsignedDst::new(unsigneds_mut, &mut self.secondary_stream);

      for stream_idx in 0..n_streams {
        if let Some(initial_value) = num_decompressor.initial_value_required(stream_idx) {
          u_dst.stream(stream_idx).fill(initial_value);
        }
      }

      let progress = num_decompressor.decompress_unsigneds(
        reader,
        error_on_insufficient_data,
        &mut u_dst,
      )?;

      for (stream_idx, delta_moments) in delta_momentss
        .iter_mut()
        .take(self.mode.n_streams())
        .enumerate()
      {
        delta_encoding::reconstruct_in_place(delta_moments, u_dst.stream(stream_idx));
      }

      join_streams(self.mode, u_dst);
      progress
    };

    unsigneds_to_nums_in_place::<T>(unsigneds_mut);

    Ok(progress)
  }

  pub fn decompress(
    &mut self,
    reader: &mut BitReader,
    error_on_insufficient_data: bool,
    num_dst: &mut [T],
  ) -> PcoResult<Progress> {
    let mut progress = Progress::default();
    while progress.n_processed < num_dst.len()
      && !progress.finished_body
      && !progress.insufficient_data
    {
      progress += self.decompress_batch(
        reader,
        error_on_insufficient_data,
        &mut num_dst[progress.n_processed..],
      )?;
    }
    Ok(progress)
  }

  pub fn bits_remaining(&self) -> usize {
    self.num_decompressor.bits_remaining()
  }
}
