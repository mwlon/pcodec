use std::cmp::min;
use std::marker::PhantomData;

use crate::bit_reader::BitReader;
use crate::chunk_metadata::DataPageMetadata;
use crate::constants::UNSIGNED_BATCH_SIZE;
use crate::data_types::{NumberLike, UnsignedLike};
use crate::delta_encoding::DeltaMoments;
use crate::errors::PcoResult;
use crate::{Mode, num_decompressor};
use crate::num_decompressor::NumDecompressor;
use crate::progress::Progress;
use crate::unsigned_src_dst::UnsignedDst;
use crate::{delta_encoding, float_mult_utils};

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
  pub(crate) fn new(data_page_meta: DataPageMetadata<T::Unsigned>) -> PcoResult<Self> {
    let delta_momentss = data_page_meta
      .streams
      .iter()
      .map(|stream| stream.delta_moments.clone())
      .collect();
    let dyn_mode = data_page_meta.mode;
    let num_decompressor = num_decompressor::new(data_page_meta)?;
    Ok(Self {
      mode: dyn_mode,
      num_decompressor,
      delta_momentss,
      secondary_stream: [T::Unsigned::ZERO; UNSIGNED_BATCH_SIZE],
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

    let progress = {
      let u_dst = UnsignedDst::new(unsigneds_mut, &mut self.secondary_stream);
      num_decompressor.decompress_unsigneds(reader, error_on_insufficient_data, u_dst)?
    };

    for stream_idx in 0..self.mode.n_streams() {
      let delta_moments = &mut delta_momentss[stream_idx];
      delta_encoding::reconstruct_in_place(delta_moments, unsigneds_mut);
    }

    {
      let u_dst = UnsignedDst::new(unsigneds_mut, &mut self.secondary_stream);
      join_streams(self.mode, u_dst);
    }

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
