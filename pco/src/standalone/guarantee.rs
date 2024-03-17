use crate::data_types::Latent;
use crate::errors::PcoResult;
use crate::standalone::constants::{
  BITS_TO_ENCODE_N_ENTRIES, BITS_TO_ENCODE_STANDALONE_VERSION, BITS_TO_ENCODE_VARINT_POWER,
  MAGIC_HEADER,
};
use crate::wrapped::guarantee as wrapped_guarantee;
use crate::{bits, PagingSpec};

pub fn header_size() -> usize {
  let max_varint_bits = BITS_TO_ENCODE_VARINT_POWER + 64;
  MAGIC_HEADER.len()
    + bits::ceil_div(
      (max_varint_bits + BITS_TO_ENCODE_STANDALONE_VERSION) as usize,
      8,
    )
    + wrapped_guarantee::header_size()
}

pub fn chunk_size<L: Latent>(n: usize) -> usize {
  1 + bits::ceil_div(BITS_TO_ENCODE_N_ENTRIES as usize, 8) + wrapped_guarantee::chunk_size::<L>(n)
}

pub fn file_size<L: Latent>(n: usize, paging_spec: &PagingSpec) -> PcoResult<usize> {
  let n_per_chunk = paging_spec.n_per_page(n)?;
  let res = header_size()
    + n_per_chunk
      .iter()
      .map(|&chunk_n| chunk_size::<L>(chunk_n))
      .sum::<usize>()
    + 1;
  Ok(res)
}

#[cfg(test)]
mod tests {
  use rand::Rng;
  use rand_xoshiro::rand_core::SeedableRng;
  use rand_xoshiro::Xoroshiro128PlusPlus;

  use crate::data_types::NumberLike;
  use crate::errors::PcoResult;
  use crate::standalone::{simple_compress, FileCompressor};
  use crate::{ChunkConfig, FloatMultSpec, PagingSpec};

  use super::*;

  #[test]
  fn test_header_guarantee() -> PcoResult<()> {
    let fc = FileCompressor::default().with_n_hint(1 << 63);
    let mut dst = Vec::new();
    fc.write_header(&mut dst)?;
    assert_eq!(header_size(), dst.len());
    Ok(())
  }

  fn check_file_guarantee<T: NumberLike>(nums: &[T], config: &ChunkConfig) -> PcoResult<()> {
    let compressed = simple_compress(nums, config)?;
    assert!(compressed.len() <= file_size::<T::L>(nums.len(), &config.paging_spec)?);
    Ok(())
  }

  #[test]
  fn test_file_guarantee_empty() -> PcoResult<()> {
    let nums = Vec::<i32>::new();
    check_file_guarantee(&nums, &ChunkConfig::default())
  }

  #[test]
  fn test_file_guarantee_antagonistic() -> PcoResult<()> {
    let mut rng = Xoroshiro128PlusPlus::seed_from_u64(0);
    let mut nums = Vec::new();
    for _i in 0..300 {
      nums.push(rng.gen_range(-1.0_f32..1.0));
    }
    let config = ChunkConfig {
      float_mult_spec: FloatMultSpec::Provided(0.1),
      delta_encoding_order: Some(5),
      paging_spec: PagingSpec::EqualPagesUpTo(10),
      ..Default::default()
    };
    check_file_guarantee(&nums, &config)
  }
}
