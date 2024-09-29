use crate::ans::AnsState;
use crate::constants::{Bitlen, Lookback, ANS_INTERLEAVING};
use crate::data_types::Latent;
use crate::metadata::delta_encoding::DeltaMoments;

#[derive(Clone, Debug)]
pub enum PageDeltaInfo<L: Latent> {
  None,
  ConsecutiveDeltaMoments(DeltaMoments<L>),
  LzDeltaLookbacks(Vec<Lookback>),
}

#[derive(Clone, Debug)]
pub struct PageInfo<L: Latent> {
  pub page_n: usize,
  pub start_idx: usize,
  pub end_idx_per_var: Vec<usize>,
  pub delta_info_per_var: Vec<PageDeltaInfo<L>>,
}

#[derive(Clone, Debug)]
pub struct DissectedPageVar<L: Latent> {
  // These vecs should have the same length.
  pub ans_vals: Vec<AnsState>,
  pub ans_bits: Vec<Bitlen>,
  pub offsets: Vec<L>,
  pub offset_bits: Vec<Bitlen>,

  pub ans_final_states: [AnsState; ANS_INTERLEAVING],
}

#[derive(Clone, Debug)]
pub struct DissectedPage<L: Latent> {
  pub page_n: usize,
  pub per_var: Vec<DissectedPageVar<L>>, // one per latent variable
}
