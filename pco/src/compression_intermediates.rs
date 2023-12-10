use crate::ans::AnsState;
use crate::constants::{Bitlen, ANS_INTERLEAVING};
use crate::data_types::UnsignedLike;
use crate::delta::DeltaMoments;

#[derive(Clone, Debug)]
pub struct PageInfo {
  pub page_n: usize,
  pub start_idx: usize,
  pub end_idx_per_var: Vec<usize>,
}

#[derive(Clone, Debug)]
pub struct PageVarLatents<U: UnsignedLike> {
  pub latents: Vec<U>,
  pub delta_moments: DeltaMoments<U>,
}

#[derive(Clone, Debug)]
pub struct PageLatents<U: UnsignedLike> {
  pub page_n: usize,
  pub per_var: Vec<PageVarLatents<U>>, // on per latent variable
}

impl<U: UnsignedLike> PageLatents<U> {
  pub fn new_pre_delta(latents_per_var: Vec<Vec<U>>) -> Self {
    let page_n = latents_per_var[0].len();
    let per_var = latents_per_var
      .into_iter()
      .map(|latents| PageVarLatents {
        latents,
        delta_moments: DeltaMoments::default(),
      })
      .collect::<Vec<_>>();
    Self { page_n, per_var }
  }
}

#[derive(Clone, Debug)]
pub struct DissectedPageVar<U: UnsignedLike> {
  // These vecs should have the same length.
  pub ans_vals: Vec<AnsState>,
  pub ans_bits: Vec<Bitlen>,
  pub offsets: Vec<U>,
  pub offset_bits: Vec<Bitlen>,

  pub ans_final_states: [AnsState; ANS_INTERLEAVING],
}

#[derive(Clone, Debug)]
pub struct DissectedPage<U: UnsignedLike> {
  pub page_n: usize,
  pub per_var: Vec<DissectedPageVar<U>>, // one per latent variable
}
