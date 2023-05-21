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
  },
  Delta {
    n: usize,
    num_decompressor: NumDecompressor<T::Unsigned>,
    delta_moments: DeltaMoments<T::Unsigned>,
    n_processed: usize,
  },
}

#[inline(never)]
fn unsigneds_to_nums_in_place<T: NumberLike>(dest: &mut [T::Unsigned]) {
  for u in dest.iter_mut() {
    *u = T::transmute_to_unsigned(T::from_unsigned(*u));
  }
}

impl<T: NumberLike> BodyDecompressor<T> {
  pub(crate) fn new(
    bin_metadata: &BinMetadata<T>,
    n: usize,
    compressed_body_size: usize,
    delta_moments: &DeltaMoments<T::Unsigned>,
  ) -> QCompressResult<Self> {
    Ok(match bin_metadata {
      BinMetadata::Simple { bins } => Self::Simple {
        num_decompressor: NumDecompressor::new(n, compressed_body_size, bins.clone())?,
      },
      BinMetadata::Delta { bins } => Self::Delta {
        n,
        num_decompressor: NumDecompressor::new(
          n.saturating_sub(delta_moments.order()),
          compressed_body_size,
          bins.clone(),
        )?,
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
      let batch_end = min(
        progress.n_processed + UNSIGNED_BATCH_SIZE,
        dest.len(),
      );
      let u_dest = T::transmute_to_unsigned_slice(&mut dest[progress.n_processed..batch_end]);
      match self {
        Self::Simple { num_decompressor } => {
          let u_progress =
            num_decompressor.decompress_unsigneds(reader, error_on_insufficient_data, u_dest)?;
          progress += u_progress;
        }
        Self::Delta {
          n,
          num_decompressor,
          delta_moments,
          n_processed,
        } => {
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
          progress.insufficient_data = u_progress.insufficient_data
        }
      };

      unsigneds_to_nums_in_place::<T>(u_dest);
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
