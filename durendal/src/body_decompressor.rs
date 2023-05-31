use std::cmp::min;
use std::marker::PhantomData;

use crate::bit_reader::BitReader;
use crate::constants::UNSIGNED_BATCH_SIZE;
use crate::data_types::{NumberLike, UnsignedLike};
use crate::delta_encoding::DeltaMoments;
use crate::errors::QCompressResult;
use crate::{ChunkMetadata, num_decompressor};
use crate::num_decompressor::NumDecompressor;
use crate::progress::Progress;
use crate::{delta_encoding, Bin};
use crate::chunk_metadata::DataPageMetadata;
use crate::modes::DynMode;

// BodyDecompressor wraps NumDecompressor and handles reconstruction from
// delta encoding.
#[derive(Clone, Debug)]
pub struct BodyDecompressor<T: NumberLike> {
  num_decompressor: Box<dyn NumDecompressor<T::Unsigned>>,
  n: usize,
  delta_moments: DeltaMoments<T::Unsigned>,
  n_processed: usize,
  phantom: PhantomData<T>,
}

#[inline(never)]
fn unsigneds_to_nums_in_place<T: NumberLike>(dest: &mut [T::Unsigned]) {
  for u in dest.iter_mut() {
    *u = T::transmute_to_unsigned(T::from_unsigned(*u));
  }
}

impl<T: NumberLike> BodyDecompressor<T> {
  pub(crate) fn new(
    mut data_page_meta: DataPageMetadata<T::Unsigned>,
    delta_moments: &DeltaMoments<T::Unsigned>,
  ) -> QCompressResult<Self> {
    let n = data_page_meta.n;
    data_page_meta.n = n.saturating_sub(delta_moments.order());
    let num_decompressor = num_decompressor::new(data_page_meta)?;
    Ok(Self {
      num_decompressor,
      n,
      n_processed: 0,
      delta_moments: delta_moments.clone(),
      phantom: PhantomData,
    })
  }

  pub fn decompress_next_batch(
    &mut self,
    reader: &mut BitReader,
    error_on_insufficient_data: bool,
    dest: &mut [T],
  ) -> QCompressResult<Progress> {
    let mut progress = Progress::default();
    while progress.n_processed < dest.len()
      && !progress.finished_body
      && !progress.insufficient_data
    {
      let batch_end = min(
        progress.n_processed + UNSIGNED_BATCH_SIZE,
        dest.len(),
      );
      let u_dest = T::transmute_to_unsigned_slice(&mut dest[progress.n_processed..batch_end]);
      let Self {
        num_decompressor,
        n,
        delta_moments,
        n_processed,
        ..
      } = self;
      let u_progress =
        num_decompressor.decompress_unsigneds(reader, error_on_insufficient_data, u_dest)?;
      let batch_size = if u_progress.finished_body {
        let batch_size = min(
          min(
            u_dest.len(),
            u_progress.n_processed + delta_moments.order(),
          ),
          *n - *n_processed,
        );
        u_dest[u_progress.n_processed..batch_size].fill(T::Unsigned::ZERO);
        batch_size
      } else {
        u_progress.n_processed
      };
      delta_encoding::reconstruct_in_place(delta_moments, u_dest);
      *n_processed += batch_size;
      progress.n_processed += batch_size;
      progress.finished_body = n_processed == n;
      progress.insufficient_data = u_progress.insufficient_data;

      unsigneds_to_nums_in_place::<T>(u_dest);
    }
    Ok(progress)
  }

  pub fn bits_remaining(&self) -> usize {
    self.num_decompressor.bits_remaining()
  }
}

#[cfg(test)]
mod tests {
  use super::BodyDecompressor;
  use crate::bin::Bin;
  use crate::bits;
  use crate::chunk_metadata::{ChunkMetadata, DataPageMetadata};
  use crate::constants::Bitlen;
  use crate::delta_encoding::DeltaMoments;
  use crate::errors::ErrorKind;
  use crate::modes::DynMode;

  fn bin_w_code(code: Vec<bool>) -> Bin<u64> {
    Bin {
      count: 1,
      code: bits::bits_to_usize(&code),
      code_len: code.len() as Bitlen,
      lower: 100,
      offset_bits: 6,
      run_len_jumpstart: None,
      gcd: 1,
      adj_bits: 0,
    }
  }

  #[test]
  fn test_corrupt_bins_error_not_panic() {
    let metadata_missing_bin = DataPageMetadata::<u64> {
      n: 2,
      compressed_body_size: 1,
      bins: &vec![bin_w_code(vec![false]), bin_w_code(vec![true, false])],
      dyn_mode: DynMode::Classic,
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
    };

    for bad_metadata in vec![metadata_missing_bin, metadata_duplicating_bin] {
      let result = BodyDecompressor::<i64>::new(
        bad_metadata,
        &DeltaMoments::default(),
      );
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
