#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeltaEncoding {
  None,
  Consecutive(usize),
}

impl DeltaEncoding {
  pub(crate) fn n_latents_per_state(&self) -> usize {
    match self {
      Self::None => 0,
      Self::Consecutive(order) => *order,
    }
  }
}
