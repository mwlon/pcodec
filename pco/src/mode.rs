use std::fmt::Debug;

use crate::data_types::UnsignedLike;
use crate::float_mult_utils::FloatMultConfig;

/// A variation of how pco serializes and deserializes numbers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Mode<U: UnsignedLike> {
  /// Each number is compressed as
  /// * which bin it's in and
  /// * the offset in that bin.
  ///
  /// Formula: bin.lower + offset
  #[default]
  Classic,
  /// Each number is compressed as
  /// * which bin it's in and
  /// * the offset in that bin as a multiplier of that bin's GCD.
  ///
  /// Formula: bin.lower + multiplier * bin.gcd
  Gcd(U),
  /// Each number is compressed as
  /// * which bin it's in,
  /// * the approximate offset in that bin as a multiplier of the base,
  /// * which bin the additional ULPs adjustment is in, and
  /// * the offset in that adjusment bin.
  ///
  /// Formula: (bin.lower + offset) * mode.base +
  /// (adj_bin.lower + adj_bin.offset) * machine_epsilon
  FloatMult(FloatMultConfig<U::Float>),
}

impl<U: UnsignedLike> Mode<U> {
  pub(crate) fn n_latent_vars(&self) -> usize {
    match self {
      Mode::Classic => 1,
      Mode::FloatMult(_) | Mode::Gcd(_) => 2,
    }
  }

  pub(crate) fn delta_order_for_latent_var(&self, latent_idx: usize, delta_order: usize) -> usize {
    match (self, latent_idx) {
      (Mode::Classic, 0) | (Mode::FloatMult(_), 0) | (Mode::Gcd(_), 0) => delta_order,
      (Mode::FloatMult(_), 1) | (Mode::Gcd(_), 1) => 0,
      _ => panic!(
        "should be unreachable; unknown latent {:?}/{}",
        self, latent_idx
      ),
    }
  }
}
