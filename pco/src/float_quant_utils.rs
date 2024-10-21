use crate::compression_intermediates::Bid;
use crate::constants::{Bitlen, QUANT_REQUIRED_BITS_SAVED_PER_NUM};
use crate::data_types::{FloatLike, Latent};
use crate::metadata::Mode;
use crate::sampling::{self, PrimaryLatentAndSavings};
use std::cmp;

const REQUIRED_QUANTIZED_PROPORTION: f64 = 0.95;

#[inline(never)]
pub(crate) fn join_latents<F: FloatLike>(k: Bitlen, primary: &mut [F::L], secondary: &[F::L]) {
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

pub(crate) fn compute_bid<F: FloatLike>(sample: &[F]) -> Option<Bid<F>> {
  let (k, freq) = estimate_best_k_and_freq(sample);
  // Nothing fancy, we simply estimate that quantizing by k bits results in
  // saving k bits per number, whenever possible. This is based on the
  // assumption that FloatQuant will usually be used on datasets that are
  // exactly quantized.
  // TODO one day, if float quant has false positives, we may want a more
  // precise estimes of bits saved. This one is overly generous when the
  // secondary latent is nontrivial.
  let bits_saved_per_infrequent_primary = freq * (k as f64);
  let bits_saved_per_num = sampling::est_bits_saved_per_num(sample, |x| {
    let primary = x.to_latent_bits() >> k;
    PrimaryLatentAndSavings {
      primary,
      bits_saved: bits_saved_per_infrequent_primary,
    }
  });
  if bits_saved_per_num > QUANT_REQUIRED_BITS_SAVED_PER_NUM {
    Some(Bid {
      mode: Mode::FloatQuant(k),
      bits_saved_per_num,
      split_fn: Box::new(move |nums| split_latents(nums, k)),
    })
  } else {
    None
  }
}

#[inline(never)]
pub(crate) fn estimate_best_k_and_freq<F: FloatLike>(sample: &[F]) -> (Bitlen, f64) {
  let thresh = (REQUIRED_QUANTIZED_PROPORTION * sample.len() as f64) as usize;
  let mut hist = vec![0; (F::PRECISION_BITS + 1) as usize];
  for x in sample {
    // Using the fact that significand bits come last in
    // the floating-point representations we care about
    let trailing_mantissa_zeros = cmp::min(F::PRECISION_BITS, x.trailing_zeros());
    hist[trailing_mantissa_zeros as usize] += 1
  }

  let mut rev_csum = 0;
  for (k, &occurrences) in hist.iter().enumerate().rev() {
    rev_csum += occurrences;
    if rev_csum >= thresh {
      return (
        k as Bitlen,
        rev_csum as f64 / sample.len() as f64,
      );
    }
  }

  unreachable!("nums should be nonempty")
}

#[cfg(test)]
mod test {
  use crate::data_types::Number;

  use super::*;

  #[test]
  fn test_estimate_best_k() {
    // all but the last of these have 21 out of 23 mantissa bits zeroed
    let mut sample = vec![1.0_f32, 1.25, -1.5, 1.75, -0.875, 0.75, 0.625].repeat(3);
    sample.push(f32::from_bits(1.0_f32.to_bits() + 1));
    let (k, freq) = estimate_best_k_and_freq(&sample);
    assert_eq!(k, 21);
    assert_eq!(
      freq,
      (sample.len() - 1) as f64 / (sample.len() as f64)
    );
    assert!(freq >= REQUIRED_QUANTIZED_PROPORTION);
  }

  #[test]
  fn test_estimate_best_k_full_precision() {
    // all elements have all 52 mantissa bits zeroed
    let sample = vec![1.0_f64; 20];
    let (k, freq) = estimate_best_k_and_freq(&sample);
    assert_eq!(k, 52);
    assert_eq!(freq, 1.0);
  }

  #[test]
  fn test_split_latents_specific_values() {
    let expected = vec![
      (
        -f32::INFINITY,
        (0b00000000000000111111111111111111, 0b00000),
      ),
      (
        -1.0f32 - f32::EPSILON,
        (0b00000010000000111111111111111111, 0b00001),
      ),
      (
        -1.0f32,
        (0b00000010000000111111111111111111, 0b00000),
      ),
      (
        -0.0f32,
        (0b00000011111111111111111111111111, 0b00000),
      ),
      (
        0.0f32,
        (0b00000100000000000000000000000000, 0b00000),
      ),
      (
        1.0f32,
        (0b00000101111111000000000000000000, 0b00000),
      ),
      (
        1.0f32 + f32::EPSILON,
        (0b00000101111111000000000000000000, 0b00001),
      ),
      (
        f32::INFINITY,
        (0b00000111111111000000000000000000, 0b00000),
      ),
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
    let nums: Vec<f64> = [-2.345f32, -1.234f32, -0.0f32, 0.0f32, 1.234f32, 2.345f32]
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

  #[test]
  fn test_compute_bid() {
    // the larger numbers in this sample have 23 - 6 = 17 bits of quantization
    let sample = (0..100).map(|x| x as f32).collect::<Vec<_>>();
    let bid = compute_bid(&sample).unwrap();
    assert!(matches!(bid.mode, Mode::FloatQuant(17)));
    assert_eq!(bid.bits_saved_per_num, 17.0);

    // same as above, except not all perfectly quantized
    let mut sample = (0..100).map(|x| x as f32).collect::<Vec<_>>();
    sample[0] += 0.1;
    sample[37] -= 0.1;
    let bid = compute_bid(&sample).unwrap();
    assert!(matches!(bid.mode, Mode::FloatQuant(17)));
    assert!(bid.bits_saved_per_num < 17.0);
    assert!(bid.bits_saved_per_num > 15.0);

    // the primary latent in this dataset has too few values and would be easily memorizable
    let sample = [0.0_f32, 1.0].repeat(50);
    let bid = compute_bid(&sample);
    assert!(bid.is_none());
  }
}
