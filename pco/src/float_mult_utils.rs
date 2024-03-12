use std::cmp::{max, min};
use std::mem;

use crate::constants::Bitlen;
use crate::data_types::SecondaryLatents;
use crate::data_types::SecondaryLatents::{Constant, Nonconstant};
use crate::data_types::{FloatLike, Latent};
use crate::{int_mult_utils, sampling};

#[inline(never)]
pub fn join_latents<F: FloatLike>(
  base: F,
  primary_dst: &[F::L],
  secondary: SecondaryLatents<F::L>,
  dst: &mut [F],
) {
  match secondary {
    Nonconstant(adjustments) => {
      for ((&mult, &adj), dst) in primary_dst
        .iter()
        .zip(adjustments.iter())
        .zip(dst.iter_mut())
      {
        let unadjusted = F::int_float_from_latent(mult) * base;
        *dst = F::from_latent_ordered(
          unadjusted
            .to_latent_ordered()
            .wrapping_add(adj)
            .wrapping_add(F::L::MID),
        )
      }
    }
    Constant(adj) => {
      let centered_adj = adj.wrapping_add(F::L::MID);
      for (&mult, dst) in primary_dst.iter().zip(dst.iter_mut()) {
        let unadjusted = F::int_float_from_latent(mult) * base;
        *dst = F::from_latent_ordered(unadjusted.to_latent_ordered().wrapping_add(centered_adj))
      }
    }
  }
}

pub fn split_latents<F: FloatLike>(page_nums: &[F], base: F, inv_base: F) -> Vec<Vec<F::L>> {
  let n = page_nums.len();
  let uninit_vec = || unsafe {
    let mut res = Vec::<F::L>::with_capacity(n);
    res.set_len(n);
    res
  };
  let mut primary = uninit_vec();
  let mut adjustments = uninit_vec();
  for (&num, (primary_dst, adj_dst)) in page_nums
    .iter()
    .zip(primary.iter_mut().zip(adjustments.iter_mut()))
  {
    let mult = (num * inv_base).round();
    *primary_dst = F::int_float_to_latent(mult);
    *adj_dst = num
      .to_latent_ordered()
      .wrapping_sub((mult * base).to_latent_ordered())
      // ULP adjustments are naturally signed quantities, so we toggle them so
      // that 0 is in the middle of the range
      .wrapping_add(F::L::MID);
  }
  vec![primary, adjustments]
}

// The rest of this file concerns automatically detecting the float `base`
// such that `x = mult * base + adj * ULP` usefully splits a delta `x` into
// latent variables `mult` and `adj` (if such a `base` exists).
//
// Somewhat different from int mult, we simplistically model that each `x` is
// a multiple of `base` with floating point errors; we would identify `base`
// for the numbers e, 2e, 3e; but if we add 1 to all the numbers, even
// though `base=e` would be just as useful in either case.
// As a result, we can think of the "loss" of an error from a multiple of base
// as O(ln|error|).
//
// I (Martin) thought about using an FFT here, but I'm not sure how to pull it
// off computationally efficiently when the frequency of interest could be in
// such a large range and must be determined so precisely.
// So instead we use an approximate Euclidean algorithm on pairs of floats.

const REQUIRED_PRECISION_BITS: Bitlen = 6;
const SNAP_THRESHOLD_ABSOLUTE: f64 = 0.02;
const SNAP_THRESHOLD_DECIMAL_RELATIVE: f64 = 0.01;
// We require that using adj bits (as opposed to full offsets between
// consecutive multiples of the base) saves at least this proportion of the
// full offsets (relative) or full uncompressed size (absolute).
const ADJ_BITS_RELATIVE_SAVINGS_THRESH: f64 = 0.5;
const ADJ_BITS_ABSOLUTE_SAVINGS_THRESH: f64 = 0.05;
const INTERESTING_TRAILING_ZEROS: u32 = 5;
const REQUIRED_TRAILING_ZEROS_FREQUENCY: f64 = 0.5;
const REQUIRED_GCD_PAIR_FREQUENCY: f64 = 0.001;

fn insignificant_float_to<F: FloatLike>(x: F) -> F {
  let spare_precision_bits = F::PRECISION_BITS.saturating_sub(REQUIRED_PRECISION_BITS) as i32;
  x * F::exp2(-spare_precision_bits)
}

fn is_approx_zero<F: FloatLike>(small: F, big: F) -> bool {
  small <= insignificant_float_to(big)
}

fn is_small_remainder<F: FloatLike>(remainder: F, original: F) -> bool {
  remainder <= original * F::exp2(-16)
}

fn is_imprecise<F: FloatLike>(value: F, err: F) -> bool {
  value <= err * F::exp2(REQUIRED_PRECISION_BITS as i32)
}

fn approx_pair_gcd<F: FloatLike>(greater: F, lesser: F) -> Option<F> {
  if is_approx_zero(lesser, greater) || lesser == greater {
    return None;
  }

  #[derive(Clone, Copy, Debug)]
  struct PairMult<F: FloatLike> {
    value: F,
    err: F,
  }

  let machine_eps = F::exp2(-(F::PRECISION_BITS as i32));
  let rem_assign = |lhs: &mut PairMult<F>, rhs: &PairMult<F>| {
    let ratio = (lhs.value / rhs.value).round();
    lhs.err += ratio * rhs.err + lhs.value * machine_eps;
    lhs.value = (lhs.value - ratio * rhs.value).abs();
  };

  let mut pair0 = PairMult {
    value: greater,
    err: F::ZERO,
  };
  let mut pair1 = PairMult {
    value: lesser,
    err: F::ZERO,
  };

  loop {
    let prev = pair0.value;
    rem_assign(&mut pair0, &pair1);
    if is_small_remainder(pair0.value, prev) || pair0.value <= pair0.err {
      return Some(pair1.value);
    }

    if is_approx_zero(pair0.value, greater) || is_imprecise(pair0.value, pair0.err) {
      return None;
    }

    mem::swap(&mut pair0, &mut pair1);
  }
}

#[inline(never)]
fn choose_candidate_base_by_trailing_zeros<F: FloatLike>(
  sample: &[F],
) -> Option<FloatMultConfig<F>> {
  let precision_bits = F::PRECISION_BITS;
  let calc_power_of_2_divisor =
    |exponent, trailing_zeros| exponent - (precision_bits.saturating_sub(trailing_zeros)) as i32;

  // the greatest k such that 2^k divides all the floats exactly
  let mut k = i32::MAX;
  let mut count = 0;
  for x in sample {
    let trailing_zeros = x.trailing_zeros();
    if *x != F::ZERO && trailing_zeros >= INTERESTING_TRAILING_ZEROS {
      let k_prime = calc_power_of_2_divisor(x.exponent(), trailing_zeros);
      count += 1;
      k = min(k, k_prime);
    }
  }

  let required_samples = max(
    (sample.len() as f64 * REQUIRED_TRAILING_ZEROS_FREQUENCY).ceil() as usize,
    sampling::MIN_SAMPLE,
  );
  if count < required_samples {
    return None;
  }

  let mut int_sample = Vec::new();
  let lshift = F::L::BITS - precision_bits - 1;
  let explicit_mantissa = F::L::MID;
  for x in sample {
    let exponent = x.exponent();
    // the greatest k' such that 2^k' divides this float exactly
    let k_prime = calc_power_of_2_divisor(x.exponent(), x.trailing_zeros());
    if k_prime >= k && exponent < k + F::L::BITS as i32 {
      let rshift = F::L::BITS - 1 - (exponent - k) as u32;
      let lshifted_w_explicit_mantissa = (x.to_latent_bits() << lshift) | explicit_mantissa;
      let multiple_of_k = lshifted_w_explicit_mantissa >> rshift;
      int_sample.push(multiple_of_k);
    }
  }

  if int_sample.len() >= required_samples {
    let int_base = int_mult_utils::choose_candidate_base(&mut int_sample).unwrap_or(F::L::ONE);
    let base = F::from_latent_numerical(int_base) * F::exp2(k);
    Some(FloatMultConfig::from_base(base))
  } else {
    None
  }
}

#[inline(never)]
fn approx_sample_gcd_euclidean<F: FloatLike>(sample: &[F]) -> Option<F> {
  let mut gcds = Vec::new();
  for i in (0..sample.len() - 1).step_by(2) {
    let a = sample[i];
    let b = sample[i + 1];
    if let Some(gcd) = approx_pair_gcd(F::max(a, b), F::min(a, b)) {
      gcds.push(gcd);
    }
  }

  let required_pairs_with_common_gcd =
    (sample.len() as f64 * REQUIRED_GCD_PAIR_FREQUENCY).ceil() as usize;
  if gcds.len() < required_pairs_with_common_gcd {
    return None;
  }

  // safe because we filtered out poorly-behaved floats
  gcds.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
  // we check a few GCDs in the middle and see if they show up frequently enough
  for percentile in [0.1, 0.3, 0.5] {
    let candidate = gcds[(percentile * gcds.len() as f64) as usize];
    let similar_gcd_count = gcds
      .iter()
      .filter(|&&gcd| (gcd - candidate).abs() < F::from_f64(0.01) * candidate)
      .count();

    if similar_gcd_count >= required_pairs_with_common_gcd {
      return Some(candidate);
    }
  }

  None
}

fn choose_candidate_base_by_euclidean<F: FloatLike>(sample: &[F]) -> Option<FloatMultConfig<F>> {
  let base = approx_sample_gcd_euclidean(sample)?;
  let base = center_sample_base(base, sample);
  let config = snap_to_int_reciprocal(base);
  Some(config)
}

#[inline(never)]
fn center_sample_base<F: FloatLike>(base: F, sample: &[F]) -> F {
  // Go back through the sample, holding all mults fixed, and adjust the gcd to
  // minimize the average deviation from mult * gcd, weighting by mult.
  // Ideally we would tweak by something between the weighted median and mode
  // of the individual tweaks, since we model loss as proportional to
  // sum[log|error|], but doing so would be computationally harder.
  let inv_base = base.inv();
  let mut tweak_sum = F::ZERO;
  let mut tweak_weight = F::ZERO;
  for &x in sample {
    let mult = (x * inv_base).round();
    let mult_exponent = mult.exponent() as Bitlen;
    if mult_exponent < F::PRECISION_BITS && mult != F::ZERO {
      let overshoot = (mult * base) - x;
      let weight = F::from_f64((F::PRECISION_BITS - mult_exponent) as f64);
      tweak_sum += weight * (overshoot / mult);
      tweak_weight += weight;
    }
  }
  base - tweak_sum / tweak_weight
}

fn snap_to_int_reciprocal<F: FloatLike>(base: F) -> FloatMultConfig<F> {
  let inv_base = base.inv();
  let round_inv_base = inv_base.round();
  let decimal_inv_base = F::from_f64(10.0_f64.powf(inv_base.to_f64().log10().round()));
  // check if relative error is below a threshold
  if (inv_base - round_inv_base).abs() < F::from_f64(SNAP_THRESHOLD_ABSOLUTE) {
    FloatMultConfig::from_inv_base(round_inv_base)
  } else if (inv_base - decimal_inv_base).abs() / inv_base
    < F::from_f64(SNAP_THRESHOLD_DECIMAL_RELATIVE)
  {
    FloatMultConfig::from_inv_base(decimal_inv_base)
  } else {
    FloatMultConfig::from_base(base)
  }
}

#[inline(never)]
fn uses_few_enough_adj_bits<F: FloatLike>(config: FloatMultConfig<F>, nums: &[F]) -> bool {
  let FloatMultConfig { base, inv_base } = config;
  let total_uncompressed_size = nums.len() * F::BITS as usize;
  let mut total_bits_saved = 0;
  let mut total_inter_base_bits = 0;
  for &x in nums {
    let mult = (x * inv_base).round();
    if mult != F::ZERO {
      let u = x.to_latent_ordered();
      // For the float 0.0, we shouldn't pretend like we're saving a
      // full PRECISION_BITS. Zero is a multiple of every possible base and
      // would get memorized by Classic if common.
      let approx = (mult * base).to_latent_ordered();
      let abs_adj = max(u, approx) - min(u, approx);
      let adj_bits = F::L::BITS - (abs_adj << 1).leading_zeros();
      let inter_base_bits = (F::PRECISION_BITS as usize).saturating_sub(mult.exponent() as usize);
      total_bits_saved += inter_base_bits.saturating_sub(adj_bits as usize);
      total_inter_base_bits += inter_base_bits;
    };
  }
  let total_bits_saved = total_bits_saved as f64;
  total_bits_saved > total_inter_base_bits as f64 * ADJ_BITS_RELATIVE_SAVINGS_THRESH
    && total_bits_saved > total_uncompressed_size as f64 * ADJ_BITS_ABSOLUTE_SAVINGS_THRESH
}

fn better_compression_than_classic<F: FloatLike>(
  config: FloatMultConfig<F>,
  sample: &[F],
  nums: &[F],
) -> bool {
  sampling::has_enough_infrequent_ints(sample, |x| {
    ((x * config.inv_base).round()).int_float_to_latent()
  }) && uses_few_enough_adj_bits(config, nums)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FloatMultConfig<F: FloatLike> {
  pub base: F,
  pub(crate) inv_base: F,
}

impl<F: FloatLike> FloatMultConfig<F> {
  fn from_base(base: F) -> Self {
    Self {
      base,
      inv_base: base.inv(),
    }
  }

  fn from_inv_base(inv_base: F) -> Self {
    Self {
      base: inv_base.inv(),
      inv_base,
    }
  }
}

fn choose_config_w_sample<F: FloatLike>(sample: &[F], nums: &[F]) -> Option<FloatMultConfig<F>> {
  let config = choose_candidate_base_by_trailing_zeros(sample)
    .or_else(|| choose_candidate_base_by_euclidean(sample))?;
  if better_compression_than_classic(config, sample, nums) {
    Some(config)
  } else {
    None
  }
}

#[inline(never)]
pub fn choose_config<F: FloatLike>(nums: &[F]) -> Option<FloatMultConfig<F>> {
  // We can compress infinities, nans, and baby floats, but we can't learn
  // the base from them.
  let sample = sampling::choose_sample(nums, |num| {
    if num.is_finite_and_normal() {
      Some(num.abs())
    } else {
      None
    }
  })?;

  choose_config_w_sample(&sample, nums)
}

#[cfg(test)]
mod test {
  use crate::data_types::OrderedLatentConvert;
  use rand::{Rng, SeedableRng};
  use std::f32::consts::{E, TAU};

  use super::*;

  fn assert_almost_equal_ulps(a: f32, b: f32, ulps_tolerance: u32, desc: &str) {
    let (a, b) = (a.to_latent_ordered(), b.to_latent_ordered());
    let udiff = max(a, b) - min(a, b);
    assert!(
      udiff <= ulps_tolerance,
      "{} far from {}; {}",
      a,
      b,
      desc,
    );
  }

  fn assert_almost_equal(a: f32, b: f32, abs_tolerance: f32, desc: &str) {
    let diff = (a - b).abs();
    assert!(
      diff <= abs_tolerance,
      "{} far from {}; {}",
      a,
      b,
      desc,
    );
  }

  fn plus_epsilons(a: f32, epsilons: i32) -> f32 {
    f32::from_latent_ordered(a.to_latent_ordered().wrapping_add(epsilons as u32))
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
  fn test_trailing_zeros() {
    assert_eq!(
      choose_candidate_base_by_trailing_zeros(&[0.0, 3.0, 6.0, 21.0, f32::exp2(100.0)].repeat(5))
        .unwrap(),
      FloatMultConfig::from_base(3.0),
    )
  }

  #[test]
  fn test_approx_pair_gcd() {
    assert_eq!(approx_pair_gcd(0.0, 0.0), None);
    assert_eq!(approx_pair_gcd(1.0, 0.0), None);
    assert_eq!(approx_pair_gcd(1.0, 1.0), None);
    assert_eq!(approx_pair_gcd(1.0, 2.0), Some(1.0));
    assert_eq!(approx_pair_gcd(6.0, 3.0), Some(3.0));
    assert_eq!(
      approx_pair_gcd(10.01_f64, 0.009999999999999787_f64),
      Some(0.009999999999999787)
    );
    assert_eq!(approx_pair_gcd(2.0_f32.powi(100), 3.0), None);
    assert_almost_equal_ulps(
      approx_pair_gcd(1.0 / 3.0, 1.0 / 4.0).unwrap(),
      1.0 / 12.0,
      1,
      "1/3 gcd 1/4",
    );
  }

  #[test]
  fn test_candidate_euclidean() {
    let nums = vec![0.0, 2.0_f32.powi(-100), 0.0037, 1.0001, f32::MAX];
    assert_almost_equal(
      choose_candidate_base_by_euclidean(&nums).unwrap().base,
      1.0E-4,
      1.0E-6,
      "10^-4 adverse",
    );
  }

  #[test]
  fn test_gcd_euclidean() {
    let nums = vec![0.0, 2.0_f32.powi(-100), 0.0037, 1.0001, f32::MAX];
    assert_almost_equal(
      approx_sample_gcd_euclidean(&nums).unwrap(),
      1.0E-4,
      1.0E-6,
      "10^-4 adverse",
    );

    let nums = vec![0.0, 2.0_f32.powi(-100), 0.0037, 0.0049, 1.0001, f32::MAX];
    assert_almost_equal(
      approx_sample_gcd_euclidean(&nums).unwrap(),
      1.0E-4,
      1.0E-9,
      "10^-4",
    );

    let nums = vec![1.0, E, TAU];
    assert_eq!(approx_sample_gcd_euclidean(&nums), None);
  }

  #[test]
  fn test_center_gcd() {
    let nums = vec![6.0 / 7.0 - 1E-4, 16.0 / 7.0 + 1E-4, 18.0 / 7.0 - 1E-4];
    assert_almost_equal(
      center_sample_base(0.28, &nums),
      2.0 / 7.0,
      1E-4,
      "center",
    )
  }

  #[test]
  fn test_snap() {
    assert_eq!(
      snap_to_int_reciprocal(0.01000333),
      FloatMultConfig {
        base: 0.01,
        inv_base: 100.0
      }
    );
    assert_eq!(
      snap_to_int_reciprocal(0.009999666),
      FloatMultConfig {
        base: 0.01,
        inv_base: 100.0
      }
    );
    assert_eq!(
      snap_to_int_reciprocal(0.143),
      FloatMultConfig {
        base: 1.0 / 7.0,
        inv_base: 7.0,
      }
    );
    assert_eq!(
      snap_to_int_reciprocal(0.0105),
      FloatMultConfig {
        base: 0.0105,
        inv_base: 1.0 / 0.0105
      }
    );
    assert_eq!(snap_to_int_reciprocal(TAU).base, TAU);
  }

  #[test]
  fn test_float_mult_better_than_classic() {
    let config = FloatMultConfig::from_inv_base(10.0);
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
    assert!(better_compression_than_classic(
      config, &nums, &nums
    ));

    for n in [10, 1000] {
      let nums = (0..n)
        .into_iter()
        .map(|x| plus_epsilons((x as f32) * 0.1, x % 2))
        .collect::<Vec<_>>();
      assert!(
        better_compression_than_classic(config, &nums, &nums),
        "n={}",
        n
      );
    }
  }

  #[test]
  fn test_float_mult_worse_than_classic() {
    let config = FloatMultConfig::from_inv_base(10.0);
    for n in [10, 1000] {
      let nums = vec![0.1; n];
      assert!(
        !better_compression_than_classic(config, &nums, &nums),
        "n={}",
        n
      );

      let nums = (0..n)
        .into_iter()
        .map(|x| (x as f32) * 0.77)
        .collect::<Vec<_>>();
      assert!(
        !better_compression_than_classic(config, &nums, &nums),
        "n={}",
        n
      );

      let nums = (0..n)
        .into_iter()
        // at this magnitude, each increment of `base` is only ~2 bits
        .map(|x| (x + 5_000_000) as f32 * 0.1)
        .collect::<Vec<_>>();
      assert!(
        !better_compression_than_classic(config, &nums, &nums),
        "n={}",
        n
      );
    }
  }

  #[test]
  fn test_float_mult_worse_than_classic_zeros() {
    let mut nums = vec![0.0_f32; 1000];
    let mut rng = rand_xoshiro::Xoroshiro128PlusPlus::seed_from_u64(0);
    let concig = FloatMultConfig::from_inv_base(1E7);
    for _ in 0..1000 {
      nums.push(rng.gen_range(0.0..1.0));
    }
    assert!(!better_compression_than_classic(
      concig, &nums, &nums
    ));
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
