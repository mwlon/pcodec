use crate::constants::Bitlen;
use crate::data_types::{FloatLike, Latent};

#[inline(never)]
pub(crate) fn join_latents<F: FloatLike>(
  k: Bitlen,
  primary: &mut [F::L],
  secondary: &[F::L],
) -> () {
  // For any float `num` such that `split_latents([num], k) == [[y], [m]]`, we have
  //     num.is_sign_positive() == (y >= sign_cutoff)
  let sign_cutoff = F::L::MID >> k;
  let lowest_k_bits_max = (F::L::ONE << k) - F::L::ONE;
  for (y_and_dst, &m) in primary.iter_mut().zip(secondary.iter()) {
    debug_assert!(
      m >> k == F::L::ZERO,
      "Invalid input to FloatQuant: m must be a k-bit integer"
    );
    let is_pos_as_float = *y_and_dst >= sign_cutoff;
    let lowest_k_bits = if is_pos_as_float {
      m
    } else {
      lowest_k_bits_max - m
    };
    *y_and_dst = (*y_and_dst << k) + lowest_k_bits;
  }
}

pub(crate) fn split_latents<F: FloatLike>(page_nums: &[F], k: Bitlen) -> Vec<Vec<F::L>> {
  let n = page_nums.len();
  let uninit_vec = || unsafe {
    let mut res = Vec::<F::L>::with_capacity(n);
    res.set_len(n);
    res
  };
  let mut primary = uninit_vec();
  let mut secondary = uninit_vec();
  let lowest_k_bits_max = (F::L::ONE << k) - F::L::ONE;
  for (&num, (primary_dst, secondary_dst)) in page_nums
    .iter()
    .zip(primary.iter_mut().zip(secondary.iter_mut()))
  {
    let num_ = num.to_latent_ordered();
    *primary_dst = num_ >> k;
    let lowest_k_bits = num_ & lowest_k_bits_max;
    // Motivation for the sign-dependent logic below:
    // In the common case where `num` is exactly quantized, we want `*secondary_dst` to always be
    // zero.  But when `num` is negative, `lowest_k_bits == lowest_k_bits_max`.  So we manually
    // flip it here, and un-flip it in `join_latents`.
    *secondary_dst = if num.is_sign_positive_() {
      lowest_k_bits
    } else {
      lowest_k_bits_max - lowest_k_bits
    };
  }
  vec![primary, secondary]
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FloatQuantConfig {
  pub k: Bitlen,
}

#[cfg(test)]
mod test {
  use crate::data_types::NumberLike;

  use super::*;

  #[test]
  fn test_split_latents_specific_values() {
    let expected = vec![
      (-f32::INFINITY, (0u32, 0u32)),
      (-0.0f32, (0u32, 0u32)),
      (0.0f32, (0u32, 0u32)),
      (-1.0f32, (0u32, 0u32)),
      (f32::INFINITY, (0u32, 0u32)),
    ];
    let (nums, (_expected_ys, _expected_ms)): (Vec<_>, (Vec<_>, Vec<_>)) =
      expected.iter().cloned().unzip();
    let k: Bitlen = 5;
    if let [ref mut ys, ms] = &mut split_latents(&nums, k)[..] {
      let actual: Vec<_> = nums
        .iter()
        .cloned()
        .zip(ys.iter().cloned().zip(ms.iter().cloned()))
        .collect();
      assert_eq!(expected, actual);
    } else {
      panic!("Bug: `split_latents` returned data in an unexpected format");
    }
  }

  #[test]
  fn test_secondary_is_zero_for_exact_quantized() {
    let k: Bitlen = f64::MANTISSA_DIGITS - f32::MANTISSA_DIGITS;
    let nums: Vec<f64> = [-1.234f32, -0.0f32, 0.0f32, 1.234f32]
      .iter()
      .map(|&num| num as f64)
      .collect();
    if let [_, ms] = &split_latents(&nums, k)[..] {
      assert!(ms.iter().all(|&m| m == 0u64));
    } else {
      panic!("Bug: `split_latents` returned data in an unexpected format");
    }
  }

  #[test]
  fn test_join_split_round_trip() {
    let nums = vec![1.234, -9999.999, f64::NAN, -f64::INFINITY];
    let uints = nums
      .iter()
      .map(|num| num.to_latent_ordered())
      .collect::<Vec<_>>();

    let k: Bitlen = 5;
    if let [ref mut ys, ms] = &mut split_latents(&nums, k)[..] {
      join_latents::<f64>(k, ys, &ms);
      assert_eq!(uints, *ys);
    } else {
      panic!("Bug: `split_latents` returned data in an unexpected format");
    }
  }
}
