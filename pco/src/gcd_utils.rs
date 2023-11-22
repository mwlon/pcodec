use crate::compression_intermediates::PageLatents;
use crate::data_types::{NumberLike, UnsignedLike};

const ARITH_CHUNK_SIZE: usize = 512;

pub fn split_latents<T: NumberLike>(nums: &[T], gcd: T::Unsigned) -> PageLatents<T::Unsigned> {
  PageLatents::new_pre_delta(vec![vec![], vec![]])
}

#[inline(never)]
pub fn join_latents<U: UnsignedLike>(gcd: U, unsigneds: &mut [U], adjustments: &[U]) {
  for (u, &adj) in unsigneds.iter_mut().zip(adjustments) {
    *u = (*u * gcd).wrapping_add(adj)
  }
}

pub fn choose_gcd<T: NumberLike>(nums: &[T]) -> Option<T::Unsigned> {
  None
}
