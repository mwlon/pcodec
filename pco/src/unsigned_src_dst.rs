use crate::ans::AnsState;

use crate::constants::{Bitlen, ANS_INTERLEAVING};
use crate::data_types::UnsignedLike;

#[derive(Clone, Debug)]
pub struct LatentSrc<U: UnsignedLike> {
  pub page_n: usize,
  pub latents: Vec<Vec<U>>, // one per latent variable
}

impl<U: UnsignedLike> LatentSrc<U> {
  pub fn new(page_n: usize, latents: Vec<Vec<U>>) -> Self {
    Self { page_n, latents }
  }
}

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
