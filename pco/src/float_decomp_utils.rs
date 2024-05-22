use std::cmp;

use crate::constants::Bitlen;
use crate::data_types::FloatLike;
use crate::sampling;

#[inline(never)]
pub(crate) fn join_latents<F: FloatLike>(k: Bitlen, primary: &mut [F::L], secondary: &[F::L]) {
  for (y_and_dst, &m) in primary.iter_mut().zip(secondary.iter()) {
    assert!(m >> k == 0, "Invalid input to FloatDecomp: m must be a k-bit integer")
    *y_and_dst = F::from_bits((*y_and_dst << k) + m).to_latent_ordered()
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
      .zip(primary.iter_mut().zip(secondary.iter_mut())) {
    let num_ = num.to_latent_bits()
    let kc = F::L::BITS - k;
    *primary_dst = num_ >> k
    *secondary_dst = (num_ << kc) >> kc
  }
  vec![primary, secondary]
}

#[inline(never)]
pub(crate) fn choose_config<F: FloatLike>(nums: &[F]) -> Option<FloatDecompConfig<F>> {
  let sample = sampling::choose_sample(nums, |num|{ Some(num) })?;
  let thresh = (0.9 * sample.len()).floor() as usize;
  let k = sample.iter()
    .map(|&x| cmp::min(F::PRECISION_BITS,
                       // Using the fact that significand bits come last in
                       // the floating-point representations we care about
                       x.to_latent_bits().trailing_zeros()))
    .enumerate()
    .rev()
    .scan(0, |csum, &(i, x)| {
      if *csum >= thresh {
        return None
      }
      *csum = *csum + x;
      Some(i)
    }).last()?
  if k > 2 {
    Some(FloatDecompConfig(k as Bitlen))
  } else {
    None
  }
}
