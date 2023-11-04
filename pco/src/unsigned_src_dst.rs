use crate::ans::AnsState;
use crate::constants::{Bitlen, ANS_INTERLEAVING};
use crate::data_types::UnsignedLike;
use crate::delta::DeltaMoments;

#[derive(Clone, Debug)]
pub struct PageVarLatents<U: UnsignedLike> {
  pub latents: Vec<U>,
  pub delta_moments: DeltaMoments<U>,
}

#[derive(Clone, Debug)]
pub struct PageLatents<U: UnsignedLike> {
  pub page_n: usize,
  // one per latent variable
  pub vars: Vec<PageVarLatents<U>>,
}

impl<U: UnsignedLike> PageLatents<U> {
  pub fn new_pre_delta(latents: Vec<Vec<U>>) -> Self {
    let page_n = latents[0].len();
    let vars = latents
      .into_iter()
      .map(|latents| PageVarLatents {
        latents,
        delta_moments: DeltaMoments::default(),
      })
      .collect::<Vec<_>>();
    Self { page_n, vars }
  }
}

#[derive(Clone, Debug)]
pub struct DissectedLatents<U: UnsignedLike> {
  // ans_vals and offsets should have the same length
  pub ans_vals: Vec<AnsState>,
  pub ans_bits: Vec<Bitlen>,
  pub offsets: Vec<U>,
  pub offset_bits: Vec<Bitlen>,
  pub ans_final_states: [AnsState; ANS_INTERLEAVING],
}

#[derive(Clone, Debug)]
pub struct DissectedSrc<U: UnsignedLike> {
  pub page_n: usize,
  pub dissected_latents: Vec<DissectedLatents<U>>, // one per latent variable
}
