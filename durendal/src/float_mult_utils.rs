use std::cmp::{max, min};
use std::collections::HashMap;
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

pub fn decode_apply_mult<U: UnsignedLike>(base: U::Float, dst: UnsignedDst<U>) {
  let (unsigneds, adjustments) = dst.decompose();
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
    unsigneds[i] = T::Unsigned::from_float_numerical(mult);
    adjustments[i] = nums[i].to_unsigned().wrapping_sub((mult * base).to_unsigned());
  }
  UnsignedSrc::new(unsigneds, adjustments)
}

const MIN_SAMPLE: usize = 10;
const SAMPLE_RATIO: usize = 40; // 1 in this many nums get put into sample
const CLASSIC_SAVINGS_RATIO: f64 = 0.4;

fn min_entropy() -> f64 {
  (MIN_SAMPLE as f64).log2()
}

fn calc_sample_n(n: usize) -> Option<usize> {
  if n >= MIN_SAMPLE {
    Some(MIN_SAMPLE + (n - MIN_SAMPLE) / SAMPLE_RATIO)
  } else {
    None
  }
}

fn choose_sample<F: FloatLike>(nums: &[F]) -> Option<Vec<F>> {
  // pick evenly-spaced nums
  let n = nums.len();
  let sample_n = calc_sample_n(n)?;

  let mut res = Vec::with_capacity(sample_n);
  for i in 0..sample_n {
    let num = nums[(i * sample_n) / n];
    // We can compress infinities, nans, and baby floats, but we can't learn
    // the GCD from them.
    if num.is_finite_and_normal() {
      res.push(num.abs());
    }
  }

  if res.len() > MIN_SAMPLE {
    Some(res)
  } else {
    None
  }
}

fn insignificant_float_to<F: FloatLike>(x: F) -> F {
  let significant_precision_bits = F::PRECISION_BITS.saturating_sub(5) as i32;
  x * F::from_f64_numerical(0.5_f64.powi(significant_precision_bits))
}

fn is_approx_zero<F: FloatLike>(small: F, big: F) -> bool {
  small <= insignificant_float_to(big)
}

fn approx_pair_gcd_uncorrected<F: FloatLike>(mut x0: F, mut x1: F) -> Option<F> {
  let greater = F::max(x0, x1);
  let thresh = insignificant_float_to(greater);
  if F::min(x0, x1) <= thresh {
    return Some(greater);
  }

  loop {
    x0 %= x1;
    if is_approx_zero(x0, x1) {
      return Some(x1);
    }

    if x0 <= thresh {
      return None;
    }

    x1 %= x0;
    if is_approx_zero(x1, x0) {
      return Some(x0);
    }

    if x1 <= thresh {
      return None;
    }
  }
}

fn approx_sample_gcd<F: FloatLike>(sample: &[F]) -> Option<F> {
  let mut maybe_gcd = approx_pair_gcd_uncorrected(sample[0], sample[1]);
  for i in 2..sample.len() {
    if let Some(gcd) = maybe_gcd {
      maybe_gcd = approx_pair_gcd_uncorrected(sample[i], gcd);
    } else {
      break;
    }
  }
  maybe_gcd
}

fn adj_bits_cutoff_to_beat_classic<U: UnsignedLike>(inv_gcd: U::Float, sample: &[U::Float], n: usize) -> Option<Bitlen> {
  // For float mult, we pay the "mult" entropy and the "adjustment" entropy
  // once per number.
  // For classic, we can memorize each mult if there are few enough and be more
  // precise around each number, paying mult entropy and a fraction of
  // adjustment entropy per number, but pay extra metadata cost per mult.
  // It's better to use float mult if both mult entropy is high (requiring
  // memorization) and
  // adj_entropy * n * class_savings < mult_entropy * bin_meta_size
  let mut counts = HashMap::<U, usize>::new();
  for &x in sample {
    let mult = U::from_float_numerical((x * inv_gcd).round());
    *counts.entry(mult).or_default() += 1;
  }
  let sample_n = sample.len();
  let mut miller_madow_entropy = (counts.len() - 1) as f64 / (sample_n as f64 *
  2.0_f64 * 2.0_f64.ln()
  );
  for &count in counts.values() {
    let p = (count as f64) / (sample_n as f64);
    miller_madow_entropy -= p * p.log2();
  }

  if miller_madow_entropy < min_entropy() {
    return None;
  }

  let rough_bin_meta_cost = U::BITS as f64 + 40.0;
  let cutoff = ((miller_madow_entropy * rough_bin_meta_cost) / (CLASSIC_SAVINGS_RATIO * n as f64)) as Bitlen;
  adj_bits_needed::<U>(inv_gcd, sample, cutoff)?;
  Some(cutoff)
}

fn center_sample_gcd<F: FloatLike>(gcd: F, sample: &[F]) -> F {
  let inv_gcd = gcd.inv();
  let mut tweaks = F::ZERO;
  for &x in sample {
    let mult = (x * inv_gcd).round();
    let overshoot = (mult * gcd) - x;
    tweaks += overshoot / mult;
  }
  gcd + tweaks / (F::from_usize_numerical(sample.len()))
}

fn snap_to_int_reciprocal<F: FloatLike>(gcd: F) -> (F, F) {
  // returns (gcd, gcd^-1)
  let inv_gcd = gcd.inv();
  let round_inv_gcd = inv_gcd.round();
  if (inv_gcd - round_inv_gcd).abs() < F::from_f64_numerical(0.001) {
    (round_inv_gcd.inv(), round_inv_gcd)
  } else {
    (gcd, inv_gcd)
  }
}


fn adj_bits_needed<U: UnsignedLike>(inv_base: U::Float, nums: &[U::Float], cutoff: Bitlen) -> Option<Bitlen> {
  let mut max_adj_bits = 0;
  let base = inv_base.inv();
  for &x in nums {
    let u = x.to_unsigned();
    let approx = ((x * inv_base).round() * base).to_unsigned();
    let adj_bits = bits::bits_to_encode_offset((max(u, approx) - min(u, approx)) << 1);
    if adj_bits > cutoff {
      return None;
    }
    max_adj_bits = max(max_adj_bits, adj_bits);
  }
  Some(max_adj_bits)
}

pub struct FloatMultConfig<F: FloatLike> {
  pub base: F,
  pub inv_base: F,
  pub adj_bits: Bitlen,
}

pub fn choose_config<T: NumberLike>(nums: &[T]) -> Option<FloatMultConfig<<T::Unsigned as UnsignedLike>::Float>> {
  let nums = T::assert_float(nums);
  let n = nums.len();
  let sample = choose_sample(nums)?;
  let gcd = approx_sample_gcd(&sample)?;
  let gcd = center_sample_gcd(gcd, &sample);
  let (gcd, inv_gcd) = snap_to_int_reciprocal(gcd);

  let adj_bits_cutoff = adj_bits_cutoff_to_beat_classic::<T::Unsigned>(inv_gcd, &sample, n)?;

  let adj_bits = adj_bits_needed::<T::Unsigned>(inv_gcd, nums, adj_bits_cutoff)?;

  Some(FloatMultConfig {
    base: gcd,
    inv_base: inv_gcd,
    adj_bits,
  })
}

// We'll only consider using FloatMultMode if we can save at least 1/this of the
// mantissa bits by using it.
// const REQUIRED_INFORMATION_GAIN_DENOM: Bitlen = 6;
// enum StrategyChainResult {
//   CloseToExactMultiple,
//   FarFromExactMultiple,
//   Uninformative, // the base is not much bigger than machine epsilon
// }
//
// struct StrategyChain<U: UnsignedLike> {
//   bases_and_invs: Vec<(U::Float, U::Float)>,
//   candidate_idx: usize,
//   pub proven_useful: bool,
//   pub adj_bits: Bitlen,
//   phantom: PhantomData<U>,
// }
//
// impl<U: UnsignedLike> StrategyChain<U> {
//   fn inv_powers_of(inv_base_0: u64, n_powers: u32) -> Self {
//     let mut inv_base = inv_base_0;
//     let mut bases_and_invs = Vec::new();
//     for _ in 0..n_powers {
//       let inv_base_float = U::Float::from_u64_numerical(inv_base);
//       bases_and_invs.push((inv_base_float.inv(), inv_base_float));
//       inv_base *= inv_base_0;
//     }
//
//     Self {
//       bases_and_invs,
//       candidate_idx: 0,
//       proven_useful: false,
//       adj_bits: 0,
//       phantom: PhantomData,
//     }
//   }
//
//   fn current_base_and_inv(&self) -> Option<(U::Float, U::Float)> {
//     self.bases_and_invs.get(self.candidate_idx).cloned()
//   }
//
//   fn current_inv_base(&self) -> Option<U::Float> {
//     self.current_base_and_inv().map(|(_, inv_base)| inv_base)
//   }
//
//   fn compatibility_with(&self, sorted_chunk: &[U::Float]) -> StrategyChainResult {
//     match self.current_base_and_inv() {
//       Some((base, inv_base)) => {
//         let mut res = StrategyChainResult::Uninformative;
//         let mut seen_mult: Option<U::Float> = None;
//         let required_information_gain = U::Float::PRECISION_BITS / REQUIRED_INFORMATION_GAIN_DENOM;
//
//         for &x in sorted_chunk {
//           let abs_float = x.abs();
//           let base_bits = U::Float::log2_epsilons_between_positives(abs_float, abs_float + base);
//           let mult = (abs_float * inv_base).round();
//           let adj_bits = U::Float::log2_epsilons_between_positives(abs_float, mult * base);
//
//           if adj_bits > base_bits.saturating_sub(required_information_gain) {
//             return StrategyChainResult::FarFromExactMultiple;
//           } else if base_bits >= required_information_gain {
//             match seen_mult {
//               Some(a_mult) if mult != a_mult => {
//                 res = StrategyChainResult::CloseToExactMultiple;
//               }
//               _ => seen_mult = Some(mult),
//             }
//           }
//         }
//
//         res
//       }
//       None => StrategyChainResult::Uninformative,
//     }
//   }
//
//   fn is_valid(&self) -> bool {
//     self.current_base_and_inv().is_some()
//   }
//
//   pub fn relax(&mut self) {
//     self.candidate_idx += 1;
//   }
// }
//
// // We'll go through all the nums and check if each one is numerically close to
// // a multiple of the first base in each chain. If not, we'll fall back to the
// // 2nd base here, and so forth, assuming that all numbers divisible by the nth
// // base are also divisible by the n+1st.
// pub struct Strategy<U: UnsignedLike> {
//   chains: Vec<StrategyChain<U>>,
// }
//
// impl<U: UnsignedLike> Strategy<U> {
//   pub fn choose_adj_bits_and_inv_base<T: NumberLike<Unsigned=U>>(&mut self, nums: &[T]) -> Option<(Bitlen, U::Float)> {
//     let floats = T::assert_float(nums);
//
//     // iterate over floats first for caching, performance
//     for chunk in floats.chunks(UNSIGNED_BATCH_SIZE) {
//       let mut any_valid = false;
//       for chain in &mut self.chains {
//         if chain.is_valid() {
//           any_valid = true;
//         } else {
//           continue;
//         }
//
//         chain.fit_to(chunk);
//       }
//
//       if !any_valid {
//         break;
//       }
//     }
//
//     self
//       .chains
//       .iter()
//       .flat_map(|chain| {
//         if chain.is_valid() {
//           chain
//             .current_inv_base()
//             .map(|inv_base| (chain.adj_bits, inv_base))
//         } else {
//           None
//         }
//       })
//       .max_by(|(_, inv_base0), (_, inv_base1)| {
//         U::Float::total_cmp(inv_base0, inv_base1)
//       })
//   }
// }
//
// impl<U: UnsignedLike> Default for Strategy<U> {
//   fn default() -> Self {
//     // 0.1, 0.01, ... 10^-9
//     Self {
//       chains: vec![StrategyChain::inv_powers_of(10, 9)],
//     }
//   }
// }

#[cfg(test)]
mod test {
  use std::ops::Mul;
  use crate::bit_words::BitWords;
  use crate::constants::Bitlen;
  use crate::data_types::NumberLike;
  use crate::modes::adjusted::AdjustedMode;

  use super::*;

  #[test]
  fn test_sample() {
    assert_eq!(calc_sample_n(9), None);
    assert_eq!(calc_sample_n(10), Some(10));
    assert_eq!(calc_sample_n(50), Some(11));
    assert_eq!(calc_sample_n(1010), Some(35));
    assert_eq!(calc_sample_n(1000010), Some(25010));
  }

  #[test]
  fn test_choose_config() {
    fn adj_bits_inv_base(floats: Vec<f64>) -> Option<(Bitlen, f64)> {
      // let mut strategy = Strategy::<u64>::default();
      // strategy.choose_adj_bits_and_inv_base(&floats)
      choose_config(&floats).map(|config| (config.adj_bits, config.inv_base))
    }

    let floats = vec![-0.1, 0.1, 0.100000000001, 0.33, 1.01, 1.1];
    assert_eq!(adj_bits_inv_base(floats), Some((0, 100.0)));

    let floats = vec![
      -f64::NEG_INFINITY,
      -f64::NAN,
      -0.1,
      1.0,
      1.1,
      f64::NAN,
      f64::INFINITY,
    ];
    assert_eq!(adj_bits_inv_base(floats), Some((0, 10.0)));

    let floats = vec![-(2.0_f64.powi(53)), -0.1, 1.0, 1.1];
    assert_eq!(adj_bits_inv_base(floats), None);

    let floats = vec![-0.1, 1.0, 1.1, 2.0_f64.powi(53)];
    assert_eq!(adj_bits_inv_base(floats), None);

    let floats = vec![1.0 / 7.0, 2.0 / 7.0];
    assert_eq!(adj_bits_inv_base(floats), None);

    let floats = vec![1.0, 1.00000000000001, 0.99999999999999];
    assert_eq!(adj_bits_inv_base(floats), None);
  }
}
