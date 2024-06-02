use crate::constants::Bitlen;
use crate::data_types::{FloatLike, Latent};

#[inline(never)]
pub(crate) fn join_latents<F: FloatLike>(
  k: Bitlen,
  primary: &mut [F::L],
  secondary: &[F::L],
) -> () {
  for (y_and_dst, &m) in primary.iter_mut().zip(secondary.iter()) {
    debug_assert!(
      m >> k == F::L::ZERO,
      "Invalid input to FloatQuant: m must be a k-bit integer"
    );
    *y_and_dst = (*y_and_dst << k) + m;
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
  for (&num, (primary_dst, secondary_dst)) in page_nums
    .iter()
    .zip(primary.iter_mut().zip(secondary.iter_mut()))
  {
    let num_ = num.to_latent_ordered();
    *primary_dst = num_ >> k;
    *secondary_dst = num_ & ((F::L::ONE << k) - F::L::ONE);
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
