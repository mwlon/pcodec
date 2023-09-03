use crate::ans::AnsState;

use crate::constants::{ANS_INTERLEAVING, Bitlen, MAX_N_LATENTS};
use crate::data_types::UnsignedLike;

#[derive(Clone, Debug)]
pub struct LatentSrc<U: UnsignedLike> {
  pub page_n: usize,
  latents: [Vec<U>; MAX_N_LATENTS],
}

impl<U: UnsignedLike> LatentSrc<U> {
  pub fn new(page_n: usize, latents: [Vec<U>; MAX_N_LATENTS]) -> Self {
    Self { page_n, latents }
  }

  pub fn latents(&self, latent_idx: usize) -> &[U] {
    &self.latents[latent_idx]
  }

  pub fn latents_mut(&mut self, stream_idx: usize) -> &mut Vec<U> {
    &mut self.latents[stream_idx]
  }
}

// #[derive(Clone, Debug)]
// pub struct Decomposed<U: ReadableUint> {
//   pub val: U,
//   pub n_bits: Bitlen,
// }

#[derive(Clone, Debug)]
pub struct DecomposedLatents<U: UnsignedLike> {
  // anss and offsets should have the same length
  pub ans_vals: Vec<AnsState>,
  pub ans_bits: Vec<Bitlen>,
  pub offsets: Vec<U>,
  pub offset_bits: Vec<Bitlen>,
  pub ans_final_states: [AnsState; ANS_INTERLEAVING],
}

#[derive(Clone, Debug)]
pub struct DecomposedSrc<U: UnsignedLike> {
  pub page_n: usize,
  pub decomposed_latents: Vec<DecomposedLatents<U>>, // one per latent variable
}
