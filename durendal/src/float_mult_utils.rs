use std::cmp::{max, min};
use std::marker::PhantomData;

use crate::{Bin, bits};
use crate::bin::BinCompressionInfo;
use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::bits::bits_to_encode_offset_bits;
use crate::constants::{Bitlen, BITS_TO_ENCODE_ADJ_BITS, UNSIGNED_BATCH_SIZE};
use crate::data_types::{FloatLike, NumberLike, UnsignedLike};
use crate::errors::QCompressResult;
use crate::modes::{Mode};
use crate::unsigned_src_dst::{UnsignedDst, UnsignedSrc};

pub fn decode_apply_mult<U: UnsignedLike>(base: U::Float, dst: &mut UnsignedDst<U>) {
  let unsigneds = dst.unsigneds_mut();
  let adjustments = dst.adjustments();
  for i in 0..unsigneds.len() {
    let unadjusted = (unsigneds[i].to_float_numerical() * base);
    unsigneds[i] = unadjusted.to_unsigned().wrapping_add(adjustments[i])
  }
}

pub fn encode_apply_mult<T: NumberLike>(
  nums: &[T],
  base: <T::Unsigned as UnsignedLike>::Float,
  inv_base: <T::Unsigned as UnsignedLike>::Float,
) -> UnsignedSrc<T::Unsigned> {
  let nums = T::assert_float(nums);
  let n = nums.len();
  let mut unsigneds = Vec::with_capacity(n);
  let mut adjustments = Vec::with_capacity(n);
  for i in 0..n {
    let mult = (nums[i] * inv_base).round();
    unsigneds[i] = mult.to_unsigned_numerical();
    adjustments[i] = nums[i].to_unsigned().wrapping_sub((mult * base).to_unsigned());
  }
  UnsignedSrc::new(unsigneds, adjustments)
}

// We'll only consider using FloatMultMode if we can save at least 1/this of the
// mantissa bits by using it.
const REQUIRED_INFORMATION_GAIN_DENOM: Bitlen = 6;

pub fn adj_bits_needed<U: UnsignedLike>(inv_base: U::Float, sorted: &[U]) -> Bitlen {
  let mut max_adj_bits = 0;
  let base = inv_base.inv();
  for &u in sorted {
    let x = U::Float::from_unsigned(u);
    let approx = ((x * inv_base).round() * base).to_unsigned();
    let adj_bits = bits::bits_to_encode_offset((max(u, approx) - min(u, approx)) << 1);
    max_adj_bits = max(max_adj_bits, adj_bits);
  }
  max_adj_bits
}

enum StrategyChainResult {
  CloseToExactMultiple,
  FarFromExactMultiple,
  Uninformative, // the base is not much bigger than machine epsilon
}

struct StrategyChain<U: UnsignedLike> {
  bases_and_invs: Vec<(U::Float, U::Float)>,
  candidate_idx: Option<usize>,
  pub proven_useful: bool,
  phantom: PhantomData<U>,
}

impl<U: UnsignedLike> StrategyChain<U> {
  fn inv_powers_of(inv_base_0: u64, n_powers: u32) -> Self {
    let mut inv_base = inv_base_0;
    let mut bases_and_invs = Vec::new();
    for _ in 0..n_powers {
      let inv_base_float = U::Float::from_u64_numerical(inv_base);
      bases_and_invs.push((inv_base_float.inv(), inv_base_float));
      inv_base *= inv_base_0;
    }

    Self {
      bases_and_invs,
      candidate_idx: Some(0),
      proven_useful: false,
      phantom: PhantomData,
    }
  }

  fn current_base_and_inv(&self) -> Option<(U::Float, U::Float)> {
    self
      .candidate_idx
      .and_then(|idx| self.bases_and_invs.get(idx).cloned())
  }

  fn current_inv_base(&self) -> Option<U::Float> {
    self.current_base_and_inv().map(|(_, inv_base)| inv_base)
  }

  fn compatibility_with(&self, sorted_chunk: &[U]) -> StrategyChainResult {
    match self.current_base_and_inv() {
      Some((base, inv_base)) => {
        let mut res = StrategyChainResult::Uninformative;
        let mut seen_mult: Option<U::Float> = None;
        let required_information_gain = U::Float::PRECISION_BITS / REQUIRED_INFORMATION_GAIN_DENOM;

        for &u in sorted_chunk {
          let abs_float = U::Float::from_unsigned(u).abs();
          let base_bits = U::Float::log2_epsilons_between_positives(abs_float, abs_float + base);
          let mult = (abs_float * inv_base).round();
          let adj_bits = U::Float::log2_epsilons_between_positives(abs_float, mult * base);

          if adj_bits > base_bits.saturating_sub(required_information_gain) {
            return StrategyChainResult::FarFromExactMultiple;
          } else if base_bits >= required_information_gain {
            match seen_mult {
              Some(a_mult) if mult != a_mult => {
                res = StrategyChainResult::CloseToExactMultiple;
              }
              _ => seen_mult = Some(mult),
            }
          }
        }

        res
      }
      None => StrategyChainResult::Uninformative,
    }
  }

  fn is_invalid(&self) -> bool {
    self.current_base_and_inv().is_none()
  }

  pub fn relax(&mut self) {
    self.candidate_idx.iter_mut().for_each(|idx| *idx += 1);
  }

  fn invalidate(&mut self) {
    self.candidate_idx = None;
  }
}

// We'll go through all the nums and check if each one is numerically close to
// a multiple of the first base in each chain. If not, we'll fall back to the
// 2nd base here, and so forth, assuming that all numbers divisible by the nth
// base are also divisible by the n+1st.
pub struct Strategy<U: UnsignedLike> {
  chains: Vec<StrategyChain<U>>,
}

impl<U: UnsignedLike> Strategy<U> {
  pub fn choose_inv_base(&mut self, sorted: &[U]) -> Option<U::Float> {
    let smallest = U::Float::from_unsigned(sorted[0]);
    let biggest = U::Float::from_unsigned(*sorted.last().unwrap());
    let biggest_float = [smallest, biggest, biggest - smallest]
      .iter()
      .map(|x| x.abs())
      .max_by(U::Float::total_cmp)
      .unwrap();

    let mut invalid_count = 0;
    for chunk in sorted.chunks(UNSIGNED_BATCH_SIZE) {
      if invalid_count == self.chains.len() {
        break;
      }

      for chain in &mut self.chains {
        if chain.is_invalid() {
          continue;
        }

        // but NANs are ok
        if biggest_float * chain.current_inv_base().unwrap() >= U::Float::GREATEST_PRECISE_INT {
          invalid_count += 1;
          chain.invalidate();
        }

        loop {
          let compatibility = chain.compatibility_with(chunk);
          match compatibility {
            StrategyChainResult::FarFromExactMultiple => chain.relax(),
            StrategyChainResult::CloseToExactMultiple => {
              chain.proven_useful = true;
              break;
            }
            _ => break,
          }
        }
      }
    }

    self
      .chains
      .iter()
      .flat_map(|chain| {
        if chain.proven_useful {
          chain.current_inv_base()
        } else {
          None
        }
      })
      .max_by(U::Float::total_cmp)
  }
}

impl<U: UnsignedLike> Default for Strategy<U> {
  fn default() -> Self {
    // 0.1, 0.01, ... 10^-9
    Self {
      chains: vec![StrategyChain::inv_powers_of(10, 9)],
    }
  }
}

#[cfg(test)]
mod test {
  use crate::bit_words::BitWords;
  use crate::constants::Bitlen;
  use crate::data_types::NumberLike;
  use crate::modes::adjusted::AdjustedMode;

  use super::*;

  #[test]
  fn test_choose_base() {
    fn inv_base(floats: Vec<f64>) -> Option<f64> {
      let mut strategy = Strategy::<u64>::default();
      let sorted = floats.iter().map(|x| x.to_unsigned()).collect::<Vec<_>>();
      strategy.choose_inv_base(&sorted)
    }

    let floats = vec![-0.1, 0.1, 0.100000000001, 0.33, 1.01, 1.1];
    assert_eq!(inv_base(floats), Some(100.0));

    let floats = vec![
      -f64::NEG_INFINITY,
      -f64::NAN,
      -0.1,
      1.0,
      1.1,
      f64::NAN,
      f64::INFINITY,
    ];
    assert_eq!(inv_base(floats), Some(10.0));

    let floats = vec![-(2.0_f64.powi(53)), -0.1, 1.0, 1.1];
    assert_eq!(inv_base(floats), None);

    let floats = vec![-0.1, 1.0, 1.1, 2.0_f64.powi(53)];
    assert_eq!(inv_base(floats), None);

    let floats = vec![1.0 / 7.0, 2.0 / 7.0];
    assert_eq!(inv_base(floats), None);

    let floats = vec![1.0, 1.00000000000001, 0.99999999999999];
    assert_eq!(inv_base(floats), None);
  }
}
