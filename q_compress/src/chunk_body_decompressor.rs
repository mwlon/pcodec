use std::cmp::min;

use crate::bit_reader::BitReader;
use crate::{ChunkMetadata, delta_encoding, num_decompressor, PrefixMetadata};
use crate::data_types::NumberLike;
use crate::delta_encoding::DeltaMoments;
use crate::errors::QCompressResult;
use crate::num_decompressor::NumDecompressor;

pub struct Numbers<T: NumberLike> {
  pub nums: Vec<T>,
  pub finished_chunk_body: bool,
}

// ChunkBodyDecompressor wraps NumDecompressor and handles reconstruction from
// delta encoding.
#[derive(Clone, Debug)]
pub enum ChunkBodyDecompressor<T: NumberLike> {
  Simple {
    num_decompressor: NumDecompressor<T::Unsigned>,
  },
  Delta {
    n: usize,
    num_decompressor: NumDecompressor<T::Unsigned>,
    delta_moments: DeltaMoments<T>,
    nums_processed: usize,
  },
}

impl<T: NumberLike> ChunkBodyDecompressor<T> {
  pub(crate) fn new(metadata: &ChunkMetadata<T>) -> QCompressResult<Self> {
    Ok(match &metadata.prefix_metadata {
      PrefixMetadata::Simple { prefixes } => Self::Simple {
        num_decompressor: num_decompressor::new(
          metadata.n,
          metadata.compressed_body_size,
          prefixes.clone()
        )?
      },
      PrefixMetadata::Delta { prefixes, delta_moments } => Self::Delta {
        n: metadata.n,
        num_decompressor: num_decompressor::new(
          metadata.n.saturating_sub(delta_moments.order()),
          metadata.compressed_body_size,
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
          finished_chunk_body: u.finished_chunk_body,
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
        let batch_size = if u_deltas.finished_chunk_body {
          min(limit, *n - *nums_processed)
        } else {
          u_deltas.unsigneds.len()
        };
        let signeds = u_deltas.unsigneds.into_iter()
          .map(T::Signed::from_unsigned)
          .collect::<Vec<_>>();
        let nums = delta_encoding::reconstruct_nums(
          delta_moments,
          &signeds,
          batch_size,
        );
        *nums_processed += batch_size;
        Ok(Numbers {
          nums,
          finished_chunk_body: nums_processed == n,
        })
      }
    }
  }

  pub fn bits_remaining(&self) -> usize {
    match self {
      Self::Simple { num_decompressor } => num_decompressor.bits_remaining(),
      Self::Delta { num_decompressor, n: _, delta_moments: _, nums_processed: _ } => num_decompressor.bits_remaining(),
    }
  }
}

#[cfg(test)]
mod tests {
  use std::marker::PhantomData;

  use super::ChunkBodyDecompressor;
  use crate::chunk_metadata::{ChunkMetadata, PrefixMetadata};
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
      phantom: PhantomData,
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
      phantom: PhantomData,
    };
    let metadata_duplicating_prefix = ChunkMetadata::<i64> {
      n: 2,
      compressed_body_size: 1,
      prefix_metadata: PrefixMetadata::Simple { prefixes: vec![
        prefix_w_code(vec![false]),
        prefix_w_code(vec![false]),
        prefix_w_code(vec![true]),
      ]},
      phantom: PhantomData,
    };

    for bad_metadata in vec![metadata_missing_prefix, metadata_duplicating_prefix] {
      let result = ChunkBodyDecompressor::new(&bad_metadata);
      match result {
        Ok(_) => panic!("expected an error for bad metadata: {:?}", bad_metadata),
        Err(e) if matches!(e.kind, ErrorKind::Corruption) => (),
        Err(e) => panic!("expected a different error than {:?} for bad metadata {:?}", e, bad_metadata),
      }
    }
  }
}
