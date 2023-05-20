use std::cmp::min;

use crate::bit_reader::BitReader;
use crate::constants::UNSIGNED_BATCH_SIZE;
use crate::data_types::{NumberLike, UnsignedLike};
use crate::delta_encoding::DeltaMoments;
use crate::errors::QCompressResult;
use crate::num_decompressor::NumDecompressor;
use crate::progress::Progress;
use crate::{delta_encoding, BinMetadata};

// BodyDecompressor wraps NumDecompressor and handles reconstruction from
// delta encoding.
#[derive(Clone, Debug)]
pub enum BodyDecompressor<T: NumberLike> {
  Simple {
    num_decompressor: NumDecompressor<T::Unsigned>,
    scratch: [T::Unsigned; UNSIGNED_BATCH_SIZE],
  },
  Delta {
    n: usize,
    num_decompressor: NumDecompressor<T::Unsigned>,
    scratch: [T::Unsigned; UNSIGNED_BATCH_SIZE],
    delta_moments: DeltaMoments<T::Signed>,
    n_processed: usize,
  },
}

#[inline(never)]
fn unsigneds_to_nums<T: NumberLike>(unsigneds: &[T::Unsigned], dest: &mut [T]) {
  // is there a better way to write this?
  for (i, &u) in unsigneds.iter().enumerate() {
    dest[i] = T::from_unsigned(u);
  }
}

impl<T: NumberLike> BodyDecompressor<T> {
  pub(crate) fn new(
    bin_metadata: &BinMetadata<T>,
    n: usize,
    compressed_body_size: usize,
    delta_moments: &DeltaMoments<T::Signed>,
  ) -> QCompressResult<Self> {
    let scratch = [T::Unsigned::ZERO; UNSIGNED_BATCH_SIZE];
    Ok(match bin_metadata {
      BinMetadata::Simple { bins } => Self::Simple {
        num_decompressor: NumDecompressor::new(n, compressed_body_size, bins.clone())?,
        scratch,
      },
      BinMetadata::Delta { bins } => Self::Delta {
        n,
        num_decompressor: NumDecompressor::new(
          n.saturating_sub(delta_moments.order()),
          compressed_body_size,
          bins.clone(),
        )?,
        scratch,
        delta_moments: delta_moments.clone(),
        n_processed: 0,
      },
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
      let limit = min(
        UNSIGNED_BATCH_SIZE,
        dest.len() - progress.n_processed,
      );
      match self {
        Self::Simple {
          num_decompressor,
          scratch,
        } => {
          let u_progress = num_decompressor.decompress_unsigneds(
            reader,
            error_on_insufficient_data,
            &mut scratch[..limit],
          )?;
          unsigneds_to_nums(
            &scratch[..u_progress.n_processed],
            &mut dest[progress.n_processed..],
          );
          progress += u_progress;
        }
        Self::Delta {
          n,
          num_decompressor,
          scratch,
          delta_moments,
          n_processed,
        } => {
          let u_progress = num_decompressor.decompress_unsigneds(
            reader,
            error_on_insufficient_data,
            &mut scratch[..limit],
          )?;
          let batch_size = min(*n - *n_processed, limit);
          if u_progress.finished_body {
            let end_fill_idx = min(batch_size, UNSIGNED_BATCH_SIZE);
            scratch[u_progress.n_processed..end_fill_idx].fill(T::Unsigned::ZERO);
          }
          delta_encoding::reconstruct_nums(
            delta_moments,
            scratch,
            batch_size,
            &mut dest[progress.n_processed..],
          );
          *n_processed += batch_size;
          progress.n_processed += batch_size;
          progress.finished_body = n_processed == n;
          progress.insufficient_data = u_progress.insufficient_data
        }
      }
    }
    Ok(progress)
  }

  pub fn bits_remaining(&self) -> usize {
    match self {
      Self::Simple {
        num_decompressor, ..
      } => num_decompressor.bits_remaining(),
      Self::Delta {
        num_decompressor, ..
      } => num_decompressor.bits_remaining(),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::BodyDecompressor;
  use crate::bin::Bin;
  use crate::bits;
  use crate::chunk_metadata::{BinMetadata, ChunkMetadata};
  use crate::constants::Bitlen;
  use crate::delta_encoding::DeltaMoments;
  use crate::errors::ErrorKind;

  fn bin_w_code(code: Vec<bool>) -> Bin<i64> {
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
    let metadata_missing_bin = ChunkMetadata::<i64> {
      n: 2,
      compressed_body_size: 1,
      bin_metadata: BinMetadata::Simple {
        bins: vec![bin_w_code(vec![false]), bin_w_code(vec![true, false])],
      },
    };
    let metadata_duplicating_bin = ChunkMetadata::<i64> {
      n: 2,
      compressed_body_size: 1,
      bin_metadata: BinMetadata::Simple {
        bins: vec![
          bin_w_code(vec![false]),
          bin_w_code(vec![false]),
          bin_w_code(vec![true]),
        ],
      },
    };

    for bad_metadata in vec![metadata_missing_bin, metadata_duplicating_bin] {
      let result = BodyDecompressor::new(
        &bad_metadata.bin_metadata,
        bad_metadata.n,
        bad_metadata.compressed_body_size,
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
