use crate::ans::AnsState;
use crate::constants::{Bitlen, ANS_INTERLEAVING};
use crate::data_types::UnsignedLike;

#[derive(Clone, Debug)]
pub struct PageInfo {
  pub page_n: usize,
  pub start_idx: usize,
  pub end_idx_per_var: Vec<usize>,
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
