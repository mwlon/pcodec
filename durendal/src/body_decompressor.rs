use std::cmp::min;
use std::marker::PhantomData;

use crate::bit_reader::BitReader;
use crate::chunk_metadata::DataPageMetadata;
use crate::constants::UNSIGNED_BATCH_SIZE;
use crate::data_types::{NumberLike, UnsignedLike};
use crate::delta_encoding;
use crate::delta_encoding::DeltaMoments;
use crate::errors::QCompressResult;
use crate::modes::DynMode;
use crate::num_decompressor;
use crate::num_decompressor::NumDecompressor;
use crate::progress::Progress;
use crate::unsigned_src_dst::UnsignedDst;

// BodyDecompressor wraps NumDecompressor and handles reconstruction from
// delta encoding.
#[derive(Clone, Debug)]
pub struct BodyDecompressor<T: NumberLike> {
  dyn_mode: DynMode<T::Unsigned>,
  num_decompressor: Box<dyn NumDecompressor<T::Unsigned>>,
  delta_moments: DeltaMoments<T::Unsigned>,
  adjustments: [T::Unsigned; UNSIGNED_BATCH_SIZE],
  phantom: PhantomData<T>,
}

#[inline(never)]
fn unsigneds_to_nums_in_place<T: NumberLike>(dest: &mut [T::Unsigned]) {
  for u in dest.iter_mut() {
    *u = T::transmute_to_unsigned(T::from_unsigned(*u));
  }
}

impl<T: NumberLike> BodyDecompressor<T> {
  pub(crate) fn new(data_page_meta: DataPageMetadata<T::Unsigned>) -> QCompressResult<Self> {
    let num_decompressor = num_decompressor::new(data_page_meta.clone())?;
    Ok(Self {
      dyn_mode: data_page_meta.dyn_mode,
      num_decompressor,
      delta_moments: data_page_meta.delta_moments,
      adjustments: [T::Unsigned::ZERO; UNSIGNED_BATCH_SIZE],
      phantom: PhantomData,
    })
  }

  #[inline(never)]
  fn decompress_batch(
    &mut self,
    reader: &mut BitReader,
    error_on_insufficient_data: bool,
    num_dst: &mut [T],
  ) -> QCompressResult<Progress> {
    let batch_end = min(UNSIGNED_BATCH_SIZE, num_dst.len());
    let unsigneds_mut = T::transmute_to_unsigned_slice(&mut num_dst[..batch_end]);
    let Self {
      num_decompressor,
      delta_moments,
      ..
    } = self;

    let progress = {
      let u_dst = UnsignedDst::new(unsigneds_mut, &mut self.adjustments);
      num_decompressor.decompress_unsigneds(reader, error_on_insufficient_data, u_dst)?
    };

    delta_encoding::reconstruct_in_place(delta_moments, unsigneds_mut);

    {
      let u_dst = UnsignedDst::new(unsigneds_mut, &mut self.adjustments);
      self.dyn_mode.finalize(u_dst);
    }

    unsigneds_to_nums_in_place::<T>(unsigneds_mut);

    Ok(progress)
  }

  pub fn decompress(
    &mut self,
    reader: &mut BitReader,
    error_on_insufficient_data: bool,
    num_dst: &mut [T],
  ) -> QCompressResult<Progress> {
    let mut progress = Progress::default();
    while progress.n_processed < num_dst.len() && !progress.finished_body && !progress.insufficient_data {
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

#[cfg(test)]
mod tests {
  use crate::bin::Bin;
  use crate::bits;
  use crate::chunk_metadata::DataPageMetadata;
  use crate::constants::Bitlen;
  use crate::delta_encoding::DeltaMoments;
  use crate::errors::ErrorKind;
  use crate::modes::DynMode;

  use super::BodyDecompressor;

  fn bin_w_code(code: Vec<bool>) -> Bin<u64> {
    Bin {
      count: 1,
      code: bits::bits_to_usize(&code),
      code_len: code.len() as Bitlen,
      lower: 100,
      offset_bits: 6,
      run_len_jumpstart: None,
      gcd: 1,
    }
  }

  #[test]
  fn test_corrupt_bins_error_not_panic() {
    let metadata_missing_bin = DataPageMetadata::<u64> {
      n: 2,
      compressed_body_size: 1,
      bins: &vec![bin_w_code(vec![false]), bin_w_code(vec![true, false])],
      dyn_mode: DynMode::Classic,
      delta_moments: DeltaMoments::default(),
    };
    let metadata_duplicating_bin = DataPageMetadata::<u64> {
      n: 2,
      compressed_body_size: 1,
      bins: &vec![
        bin_w_code(vec![false]),
        bin_w_code(vec![false]),
        bin_w_code(vec![true]),
      ],
      dyn_mode: DynMode::Classic,
      delta_moments: DeltaMoments::default(),
    };

    for bad_metadata in vec![metadata_missing_bin, metadata_duplicating_bin] {
      let result = BodyDecompressor::<i64>::new(bad_metadata.clone());
      match result {
        Ok(_) => panic!(
          "expected an error for bad metadata: {:?}",
          bad_metadata
        ),
        Err(e) if matches!(e.kind, ErrorKind::Corruption) => (),
        Err(e) => panic!(
          "expected a different error than {:?} for bad metadata {:?}",
          e, bad_metadata
        ),
      }
    }
  }
}
