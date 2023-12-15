use std::cmp::{max, min};

use crate::constants::Bitlen;
use crate::data_types::{FloatLike, NumberLike, UnsignedLike};
use crate::wrapped::SecondaryLatents;
use crate::wrapped::SecondaryLatents::{Constant, Nonconstant};
use crate::{delta, sampling};

const ARITH_CHUNK_SIZE: usize = 512;

// PageDecompressor is already doing batching, so we don't need to here
#[inline(never)]
pub(crate) fn join_latents<U: UnsignedLike>(
  base: U::Float,
  primary_dst: &mut [U],
  secondary: SecondaryLatents<U>,
) {
  match secondary {
    Nonconstant(adjustments) => {
      delta::toggle_center_in_place(adjustments);
      for (u, &adj) in primary_dst.iter_mut().zip(adjustments.iter()) {
        let unadjusted = u.to_int_float() * base;
        *u = unadjusted.to_unsigned().wrapping_add(adj)
      }
    }
    Constant(adj) => {
      let adj = adj.wrapping_add(U::MID);
      for u in primary_dst.iter_mut() {
        let unadjusted = u.to_int_float() * base;
        *u = unadjusted.to_unsigned().wrapping_add(adj)
      }
    }
  }
}

// compressor doesn't batch, so we do that ourselves for efficiency
pub fn split_latents<T: NumberLike>(
  page_nums: &[T],
  base: <T::Unsigned as UnsignedLike>::Float,
  inv_base: <T::Unsigned as UnsignedLike>::Float,
) -> Vec<Vec<T::Unsigned>> {
  let page_nums = T::assert_float(page_nums);
  let n = page_nums.len();
  let uninit_vec = || unsafe {
    let mut res = Vec::<T::Unsigned>::with_capacity(n);
    res.set_len(n);
    res
  };
  let mut primary = uninit_vec();
  let mut adjustments = uninit_vec();
  let mut mults = [<T::Unsigned as UnsignedLike>::Float::ZERO; ARITH_CHUNK_SIZE];
  let mut base_i = 0;
  for chunk in page_nums.chunks(ARITH_CHUNK_SIZE) {
    for i in 0..chunk.len() {
      mults[i] = (chunk[i] * inv_base).round();
    }
    for i in 0..chunk.len() {
      primary[base_i + i] = T::Unsigned::from_int_float(mults[i]);
    }
    for i in 0..chunk.len() {
      adjustments[base_i + i] = chunk[i]
        .to_unsigned()
        .wrapping_sub((mults[i] * base).to_unsigned());
    }
    delta::toggle_center_in_place(&mut adjustments[base_i..base_i + chunk.len()]);
    base_i += ARITH_CHUNK_SIZE;
  }
  vec![primary, adjustments]
}

// # of bins before classic can't memorize them anymore, even if it tried
const NEAR_ZERO_MACHINE_EPSILON_BITS: Bitlen = 6;
const SNAP_THRESHOLD_ABSOLUTE: f64 = 0.02;
const SNAP_THRESHOLD_DECIMAL_RELATIVE: f64 = 0.01;
// We require that using adj bits (as opposed to full offsets between
// consecutive multiples of the base) saves at least this proportion of the
// full offsets (relative) or full uncompressed size (absolute).
const ADJ_BITS_RELATIVE_SAVINGS_THRESH: f64 = 0.5;
const ADJ_BITS_ABSOLUTE_SAVINGS_THRESH: f64 = 0.2;

fn insignificant_float_to<F: FloatLike>(x: F) -> F {
  let significant_precision_bits =
    F::PRECISION_BITS.saturating_sub(NEAR_ZERO_MACHINE_EPSILON_BITS) as i32;
  x * F::from_f64(2.0_f64.powi(-significant_precision_bits))
}

fn is_approx_zero<F: FloatLike>(small: F, big: F) -> bool {
  small <= insignificant_float_to(big)
}

fn is_small_remainder<F: FloatLike>(remainder: F, original: F) -> bool {
  remainder <= original * F::from_f64(2.0_f64.powi(-16))
}

fn approx_pair_gcd_uncorrected<F: FloatLike>(greater: F, lesser: F, median: F) -> Option<F> {
  if is_approx_zero(lesser, median) {
    return Some(greater);
  } else if is_approx_zero(lesser, greater) {
    return Some(lesser);
  }

  #[derive(Clone, Copy, Debug)]
  struct PairMult<F: FloatLike> {
    value: F,
    abs_value: F,
    mult0: F,
    mult1: F,
  }

  // TODO is this actually more numerically stable than the obvious algorithm?
  let rem_assign = |lhs: &mut PairMult<F>, rhs: &PairMult<F>| {
    let ratio = (lhs.value / rhs.value).round();
    lhs.mult0 -= ratio * rhs.mult0;
    lhs.mult1 -= ratio * rhs.mult1;
    lhs.value = lhs.mult0 * greater + lhs.mult1 * lesser;
    lhs.abs_value = lhs.value.abs()
  };

  let mut pair0 = PairMult {
    value: greater,
    abs_value: greater,
    mult0: F::ONE,
    mult1: F::ZERO,
  };
  let mut pair1 = PairMult {
    value: lesser,
    abs_value: lesser,
    mult0: F::ZERO,
    mult1: F::ONE,
  };

  loop {
    let prev = pair0.abs_value;
    rem_assign(&mut pair0, &pair1);
    if is_small_remainder(pair0.abs_value, prev) {
      return Some(pair1.abs_value);
    }

    // for numerical stability, we need the following to be accurate:
    // |pair0.mult0 * greater - pair1.mult1 * lesser|
    // (that's pair0.abs_value)
    if is_approx_zero(
      pair0.abs_value,
      F::max(median, (pair0.mult0 * greater).abs()),
    ) {
      return None;
    }

    let prev = pair1.abs_value;
    rem_assign(&mut pair1, &pair0);
    if is_small_remainder(pair1.abs_value, prev) {
      return Some(pair0.abs_value);
    }

    if is_approx_zero(
      pair1.abs_value,
      F::max(median, (pair1.mult1 * lesser).abs()),
    ) {
      return None;
    }
  }
}

fn approx_sample_gcd<F: FloatLike>(sample: &[F]) -> Option<F> {
  let mut maybe_gcd = Some(F::ZERO);
  let median = sample[sample.len() / 2];
  for &x in sample {
    if let Some(gcd) = maybe_gcd {
      maybe_gcd = approx_pair_gcd_uncorrected(x, gcd, median);
    } else {
      break;
    }
  }
  maybe_gcd
}

fn center_sample_gcd<F: FloatLike>(gcd: F, sample: &[F]) -> F {
  let inv_gcd = gcd.inv();
  let mut min_tweak = F::MAX;
  let mut max_tweak = F::MIN;
  for &x in sample {
    let mult = (x * inv_gcd).round();
    let overshoot = (mult * gcd) - x;
    min_tweak = F::min(min_tweak, overshoot / mult);
    max_tweak = F::max(max_tweak, overshoot / mult);
  }
  gcd - (min_tweak + max_tweak) / F::from_f64(2.0_f64)
}

fn snap_to_int_reciprocal<F: FloatLike>(gcd: F) -> (F, F) {
  // returns (gcd, gcd^-1)
  let inv_gcd = gcd.inv();
  let round_inv_gcd = inv_gcd.round();
  let decimal_inv_gcd = F::from_f64(10.0_f64.powf(inv_gcd.to_f64().log10().round()));
  // check if relative error is below a threshold
  if (inv_gcd - round_inv_gcd).abs() < F::from_f64(SNAP_THRESHOLD_ABSOLUTE) {
    (round_inv_gcd.inv(), round_inv_gcd)
  } else if (inv_gcd - decimal_inv_gcd).abs() / inv_gcd
    < F::from_f64(SNAP_THRESHOLD_DECIMAL_RELATIVE)
  {
    (decimal_inv_gcd.inv(), decimal_inv_gcd)
  } else {
    (gcd, inv_gcd)
  }
}

// TODO there is redundant work between this and split_latents
fn uses_few_enough_adj_bits<U: UnsignedLike>(inv_base: U::Float, nums: &[U::Float]) -> bool {
  let base = inv_base.inv();
  let total_uncompressed_size = nums.len() * U::BITS as usize;
  let mut total_bits_saved = 0;
  let mut total_inter_base_bits = 0;
  for &x in nums {
    let u = x.to_unsigned();
    let mult = (x * inv_base).round();
    let approx = (mult * base).to_unsigned();
    let abs_adj = max(u, approx) - min(u, approx);
    let adj_bits = U::BITS - (abs_adj << 1).leading_zeros();
    let inter_base_bits =
      (U::Float::PRECISION_BITS as usize).saturating_sub(max(mult.exponent(), 0) as usize);
    total_bits_saved += inter_base_bits.saturating_sub(adj_bits as usize);
    total_inter_base_bits += inter_base_bits;
  }
  let total_bits_saved = total_bits_saved as f64;
  total_bits_saved > total_inter_base_bits as f64 * ADJ_BITS_RELATIVE_SAVINGS_THRESH
    && total_bits_saved > total_uncompressed_size as f64 * ADJ_BITS_ABSOLUTE_SAVINGS_THRESH
}

fn better_compression_than_classic<U: UnsignedLike>(
  inv_gcd: U::Float,
  sample: &[U::Float],
  nums: &[U::Float],
) -> bool {
  sampling::has_enough_infrequent_ints(sample, |x| {
    U::from_int_float((x * inv_gcd).round())
  }) && uses_few_enough_adj_bits::<U>(inv_gcd, nums)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FloatMultConfig<F: FloatLike> {
  pub base: F,
  pub(crate) inv_base: F,
}

fn choose_config_w_sample<U: UnsignedLike>(
  sample: &[U::Float],
  nums: &[U::Float],
) -> Option<FloatMultConfig<U::Float>> {
  let gcd = approx_sample_gcd(sample)?;
  let gcd = center_sample_gcd(gcd, sample);
  let (gcd, inv_gcd) = snap_to_int_reciprocal(gcd);

  if !better_compression_than_classic::<U>(inv_gcd, sample, nums) {
    return None;
  }

  Some(FloatMultConfig {
    base: gcd,
    inv_base: inv_gcd,
  })
}

pub fn choose_config<T: NumberLike>(
  nums: &[T],
) -> Option<FloatMultConfig<<T::Unsigned as UnsignedLike>::Float>> {
  let nums = T::assert_float(nums);
  // We can compress infinities, nans, and baby floats, but we can't learn
  // the GCD from them.
  let mut sample = sampling::choose_sample(nums, |num| {
    if num.is_finite_and_normal() {
      Some(num.abs())
    } else {
      None
    }
  })?;

  // this is valid since all the x's are well behaved
  sample.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
  choose_config_w_sample::<T::Unsigned>(&sample, nums)
}

#[cfg(test)]
mod test {
  use std::f32::consts::{E, TAU};

  use super::*;

  fn assert_almost_equal_me(a: f32, b: f32, machine_epsilon_tolerance: u32, desc: &str) {
    let (a, b) = (a.to_unsigned(), b.to_unsigned());
    let udiff = max(a, b) - min(a, b);
    assert!(
      udiff <= machine_epsilon_tolerance,
      "{} far from {}; {}",
      a,
      b,
      desc
    );
  }

  fn assert_almost_equal(a: f32, b: f32, abs_tolerance: f32, desc: &str) {
    let diff = (a - b).abs();
    assert!(
      diff <= abs_tolerance,
      "{} far from {}; {}",
      a,
      b,
      desc
    );
  }

  fn plus_epsilons(a: f32, epsilons: i32) -> f32 {
    f32::from_unsigned(a.to_unsigned().wrapping_add(epsilons as u32))
  }

  #[test]
  fn test_near_zero() {
    assert_eq!(
      insignificant_float_to(1.0_f64),
      1.0 / ((1_u64 << 46) as f64)
    );
    assert_eq!(
      insignificant_float_to(1.0_f32),
      1.0 / ((1_u64 << 17) as f32)
    );
    assert_eq!(
      insignificant_float_to(32.0_f32),
      1.0 / ((1_u64 << 12) as f32)
    );
  }

  #[test]
  fn test_approx_pair_gcd() {
    assert_eq!(
      approx_pair_gcd_uncorrected(0.0, 0.0, 1.0),
      Some(0.0)
    );
    assert_eq!(
      approx_pair_gcd_uncorrected(1.0, 0.0, 1.0),
      Some(1.0)
    );
    assert_eq!(
      approx_pair_gcd_uncorrected(1.0, 1.0, 1.0),
      Some(1.0)
    );
    assert_eq!(
      approx_pair_gcd_uncorrected(6.0, 3.0, 1.0),
      Some(3.0)
    );
    assert_eq!(
      approx_pair_gcd_uncorrected(10.01_f64, 0.009999999999999787_f64, 1.0_f64),
      Some(0.009999999999999787)
    );
    // 2^100 is not a multiple of 3, but it's certainly within machine epsilon of one
    assert_eq!(
      approx_pair_gcd_uncorrected(2.0_f32.powi(100), 3.0, 1.0),
      Some(3.0)
    );
    // in this case, the median is big, so assume the lhs of 3 is just a numerical error
    assert_eq!(
      approx_pair_gcd_uncorrected(2.0_f32.powi(100), 3.0, 2.0_f32.powi(99)),
      Some(2.0_f32.powi(100))
    );
    assert_almost_equal_me(
      approx_pair_gcd_uncorrected(1.0 / 3.0, 1.0 / 4.0, 1.0).unwrap(),
      1.0 / 12.0,
      1,
      "1/3 gcd 1/4",
    );
  }

  #[test]
  fn test_approx_sample_gcd() {
    let nums = vec![0.0, 2.0_f32.powi(-100), 0.0037, 1.0001, f32::MAX];
    assert_almost_equal(
      approx_sample_gcd(&nums).unwrap(),
      1.0E-4,
      1.0E-6,
      "10^-4 adverse",
    );

    let nums = vec![0.0, 2.0_f32.powi(-100), 0.0037, 0.0049, 1.0001, f32::MAX];
    assert_almost_equal(
      approx_sample_gcd(&nums).unwrap(),
      1.0E-4,
      1.0E-9,
      "10^-4",
    );

    let nums = vec![
      0.0,
      2.0_f32.powi(-100),
      0.0037,
      1.0001,
      1.000_333_3,
      f32::MAX,
    ];
    assert_eq!(approx_sample_gcd(&nums), None);

    let nums = vec![1.0, E, TAU];
    assert_eq!(approx_sample_gcd(&nums), None);
  }

  #[test]
  fn test_center_gcd() {
    let nums = vec![6.0 / 7.0 - 1E-4, 16.0 / 7.0 + 1E-4, 18.0 / 7.0 - 1E-4];
    assert_almost_equal(
      center_sample_gcd(0.28, &nums),
      2.0 / 7.0,
      1E-4,
      "center",
    )
  }

  #[test]
  fn test_snap() {
    assert_eq!(
      snap_to_int_reciprocal(0.01000333),
      (0.01, 100.0)
    );
    assert_eq!(
      snap_to_int_reciprocal(0.009999666),
      (0.01, 100.0)
    );
    assert_eq!(
      snap_to_int_reciprocal(0.143),
      (1.0 / 7.0, 7.0)
    );
    assert_eq!(
      snap_to_int_reciprocal(0.0105),
      (0.0105, 1.0 / 0.0105)
    );
    assert_eq!(snap_to_int_reciprocal(TAU).0, TAU);
  }

  #[test]
  fn test_float_mult_better_than_classic() {
    let nums = vec![
      f32::NEG_INFINITY,
      -f32::NAN,
      -999.0,
      -0.3,
      0.0,
      0.1,
      0.2,
      0.3,
      0.3,
      0.4,
      0.5,
      0.6,
      0.7,
      f32::NAN,
      f32::INFINITY,
    ];
    assert!(better_compression_than_classic::<u32>(
      10.0, &nums, &nums
    ));

    for n in [10, 1000] {
      let nums = (0..n)
        .into_iter()
        .map(|x| plus_epsilons((x as f32) * 0.1, x % 2))
        .collect::<Vec<_>>();
      assert!(
        better_compression_than_classic::<u32>(10.0, &nums, &nums),
        "n={}",
        n
      );
    }
  }

  #[test]
  fn test_float_mult_worse_than_classic() {
    for n in [10, 1000] {
      let nums = vec![0.1; n];
      assert!(
        !better_compression_than_classic::<u32>(10.0, &nums, &nums),
        "n={}",
        n
      );

      let nums = (0..n)
        .into_iter()
        .map(|x| (x as f32) * 0.77)
        .collect::<Vec<_>>();
      assert!(
        !better_compression_than_classic::<u32>(10.0, &nums, &nums),
        "n={}",
        n
      );

      let nums = (0..n)
        .into_iter()
        .map(|x| (x + 200000) as f32 * 0.1)
        .collect::<Vec<_>>();
      assert!(
        !better_compression_than_classic::<u32>(10.0, &nums, &nums),
        "n={}",
        n
      );
    }
  }

  #[test]
  fn test_choose_config() {
    let mut sevenths = Vec::new();
    let mut ones = Vec::new();
    let mut noisy_decimals = Vec::new();
    let mut junk = Vec::new();
    for i in 0..1000 {
      sevenths.push(((i % 50) - 20) as f32 * (1.0 / 7.0));
      ones.push(1.0);
      noisy_decimals.push(plus_epsilons(
        0.1 * ((i - 100) as f32),
        -7 + i % 15,
      ));
      junk.push((i as f32).sin());
    }

    assert_eq!(
      choose_config(&sevenths),
      Some(FloatMultConfig {
        base: 1.0 / 7.0,
        inv_base: 7.0,
      })
    );
    assert_eq!(choose_config(&ones), None);
    assert_eq!(
      choose_config(&noisy_decimals),
      Some(FloatMultConfig {
        base: 1.0 / 10.0,
        inv_base: 10.0,
      })
    );
    assert_eq!(choose_config(&junk), None);
  }
}
