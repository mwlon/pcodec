use std::cmp::min;

use crate::bit_reader::BitReader;
use crate::{delta_encoding, PrefixMetadata};
use crate::data_types::NumberLike;
use crate::delta_encoding::DeltaMoments;
use crate::errors::QCompressResult;
use crate::num_decompressor::NumDecompressor;

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
    prefix_metadata: &PrefixMetadata<T>,
    n: usize,
    compressed_body_size: usize,
    delta_moments: &DeltaMoments<T::Signed>,
  ) -> QCompressResult<Self> {
    Ok(match prefix_metadata {
      PrefixMetadata::Simple { prefixes } => Self::Simple {
        num_decompressor: NumDecompressor::new(
          n,
          compressed_body_size,
          prefixes.clone()
        )?
      },
      PrefixMetadata::Delta { prefixes } => Self::Delta {
        n,
        num_decompressor: NumDecompressor::new(
          n.saturating_sub(delta_moments.order()),
          compressed_body_size,
          prefixes.clone()
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
      Self::Simple { num_decompressor } => num_decompressor.decompress_unsigneds_limited(
        reader,
        limit,
        error_on_insufficient_data,
      ).map(|u| {
        Numbers {
          nums: u.unsigneds.into_iter().map(T::from_unsigned).collect(),
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
        let (nums, new_delta_moments) = delta_encoding::reconstruct_nums(
          delta_moments,
          &u_deltas.unsigneds,
          batch_size,
        );
        *nums_processed += batch_size;
        *delta_moments = new_delta_moments;
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
      Self::Delta { num_decompressor, .. } => num_decompressor.bits_remaining(),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::BodyDecompressor;
  use crate::chunk_metadata::{ChunkMetadata, PrefixMetadata};
  use crate::delta_encoding::DeltaMoments;
  use crate::errors::ErrorKind;
  use crate::prefix::Prefix;

  fn prefix_w_code(code: Vec<bool>) -> Prefix<i64> {
    Prefix {
      count: 1,
      code,
      lower: 100,
      upper: 200,
      run_len_jumpstart: None,
      gcd: 1,
    }
  }

  #[test]
  fn test_corrupt_prefixes_error_not_panic() {
    let metadata_missing_prefix = ChunkMetadata::<i64> {
      n: 2,
      compressed_body_size: 1,
      prefix_metadata: PrefixMetadata::Simple { prefixes: vec![
        prefix_w_code(vec![false]),
        prefix_w_code(vec![true, false]),
      ]},
      delta_moments: DeltaMoments::default(),
    };
    let metadata_duplicating_prefix = ChunkMetadata::<i64> {
      n: 2,
      compressed_body_size: 1,
      prefix_metadata: PrefixMetadata::Simple { prefixes: vec![
        prefix_w_code(vec![false]),
        prefix_w_code(vec![false]),
        prefix_w_code(vec![true]),
      ]},
      delta_moments: DeltaMoments::default(),
    };

    for bad_metadata in vec![metadata_missing_prefix, metadata_duplicating_prefix] {
      let result = BodyDecompressor::new(
        &bad_metadata.prefix_metadata,
        bad_metadata.n,
        bad_metadata.compressed_body_size,
        &DeltaMoments::default(),
      );
      match result {
        Ok(_) => panic!("expected an error for bad metadata: {:?}", bad_metadata),
        Err(e) if matches!(e.kind, ErrorKind::Corruption) => (),
        Err(e) => panic!("expected a different error than {:?} for bad metadata {:?}", e, bad_metadata),
      }
    }
  }
}
