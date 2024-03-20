use crate::data_types::Latent;
use crate::{Bin, ChunkLatentVarMeta, ChunkMeta, Mode};

/// Returns the maximum possible byte size of a wrapped header.
pub fn header_size() -> usize {
  1
}

pub(crate) fn baseline_chunk_meta<L: Latent>() -> ChunkMeta<L> {
  ChunkMeta {
    mode: Mode::Classic,
    delta_encoding_order: 0,
    per_latent_var: vec![ChunkLatentVarMeta {
      ans_size_log: 0,
      bins: vec![Bin {
        weight: 1,
        lower: L::ZERO,
        offset_bits: L::BITS,
      }],
    }],
  }
}

/// Returns the maximum possible byte size of a wrapped chunk for a given
/// latent type (e.g. u32 or u64) and count of numbers.
pub fn chunk_size<L: Latent>(n: usize) -> usize {
  // TODO if we ever add NumberLikes that are smaller than their Latents, we
  // may want to make this more generic
  baseline_chunk_meta::<L>().exact_size() + n * L::BITS.div_ceil(8) as usize
}

#[cfg(test)]
mod tests {
  use rand::Rng;
  use rand_xoshiro::rand_core::SeedableRng;
  use rand_xoshiro::Xoroshiro128PlusPlus;

  use crate::data_types::NumberLike;
  use crate::errors::PcoResult;
  use crate::wrapped::FileCompressor;
  use crate::{ChunkConfig, FloatMultSpec, PagingSpec};

  use super::*;

  #[test]
  fn test_header_guarantee() -> PcoResult<()> {
    let fc = FileCompressor::default();
    let mut dst = Vec::new();
    fc.write_header(&mut dst)?;
    assert_eq!(header_size(), dst.len());
    Ok(())
  }

  fn check_chunk_guarantee<T: NumberLike>(nums: &[T], config: &ChunkConfig) -> PcoResult<()> {
    let n = nums.len();
    let n_pages = config.paging_spec.n_per_page(n)?.len();
    let mut dst = Vec::new();
    let fc = FileCompressor::default();
    let cc = fc.chunk_compressor(nums, config)?;
    cc.write_chunk_meta(&mut dst)?;
    for i in 0..n_pages {
      cc.write_page(i, &mut dst)?;
    }
    assert!(dst.len() <= chunk_size::<T::L>(n));
    Ok(())
  }

  #[test]
  fn test_chunk_guarantee_uniform() -> PcoResult<()> {
    let mut rng = Xoroshiro128PlusPlus::seed_from_u64(0);
    let mut nums = Vec::new();
    for _ in 0..100 {
      nums.push(rng.gen_range(0_u32..u32::MAX));
    }
    let config = ChunkConfig {
      paging_spec: PagingSpec::EqualPagesUpTo(10),
      ..Default::default()
    };
    check_chunk_guarantee(&nums, &config)
  }

  #[test]
  fn test_chunk_guarantee_antagonistic() -> PcoResult<()> {
    let mut rng = Xoroshiro128PlusPlus::seed_from_u64(0);
    let mut nums = Vec::new();
    for _ in 0..300 {
      nums.push(rng.gen_range(-1.0..1.0));
    }
    let config = ChunkConfig {
      float_mult_spec: FloatMultSpec::Provided(0.1),
      delta_encoding_order: Some(5),
      paging_spec: PagingSpec::EqualPagesUpTo(10),
      ..Default::default()
    };
    check_chunk_guarantee(&nums, &config)
  }
}
