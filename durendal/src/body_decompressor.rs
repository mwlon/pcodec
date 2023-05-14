use std::cmp::min;

use crate::bit_reader::BitReader;
use crate::data_types::NumberLike;
use crate::delta_encoding::DeltaMoments;
use crate::errors::QCompressResult;
use crate::num_decompressor::NumDecompressor;
use crate::{delta_encoding, BinMetadata};

#[derive(Debug)]
pub struct Numbers<T: NumberLike> {
  pub nums: Vec<T>,
  pub finished_body: bool,
}

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
    delta_moments: DeltaMoments<T::Signed>,
    nums_processed: usize,
  },
}

impl<T: NumberLike> BodyDecompressor<T> {
  pub(crate) fn new(
    bin_metadata: &BinMetadata<T>,
    n: usize,
    compressed_body_size: usize,
    delta_moments: &DeltaMoments<T::Signed>,
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
        nums_processed: 0,
      },
    })
  }

  pub fn decompress_next_batch(
    &mut self,
    reader: &mut BitReader,
    limit: usize,
    error_on_insufficient_data: bool,
  ) -> QCompressResult<Numbers<T>> {
    match self {
      Self::Simple { num_decompressor } => num_decompressor
        .decompress_unsigneds_limited(reader, limit, error_on_insufficient_data)
        .map(|u| {
          let nums = u.unsigneds.into_iter().map(T::from_unsigned).collect();
          Numbers {
            nums,
            finished_body: u.finished_body,
          }
        }),
      Self::Delta {
        n,
        num_decompressor,
        delta_moments,
        nums_processed,
      } => {
        let u_deltas = num_decompressor.decompress_unsigneds_limited(
          reader,
          limit,
          error_on_insufficient_data,
        )?;
        let batch_size = if u_deltas.finished_body {
          min(limit, *n - *nums_processed)
        } else {
          u_deltas.unsigneds.len()
        };
        let nums = delta_encoding::reconstruct_nums(delta_moments, u_deltas.unsigneds, batch_size);
        *nums_processed += batch_size;
        Ok(Numbers {
          nums,
          finished_body: nums_processed == n,
        })
      }
    }
  }

  pub fn bits_remaining(&self) -> usize {
    match self {
      Self::Simple { num_decompressor } => num_decompressor.bits_remaining(),
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
  use crate::delta_encoding::DeltaMoments;
  use crate::errors::ErrorKind;

  fn bin_w_code(code: Vec<bool>) -> Bin<i64> {
    Bin {
      count: 1,
      code: bits::bits_to_usize(&code),
      code_len: code.len(),
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
