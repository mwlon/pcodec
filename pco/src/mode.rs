use std::fmt::Debug;

use crate::constants::Bitlen;
use crate::data_types::{FloatLike, Latent};

// Internally, here's how we should model each mode:
//
// Classic: The data is drawn from a smooth distribution.
//   Most natural data is like this.
//
// IntMult: The data is generated by 2 smooth distributions:
//   one whose outputs are multiplied by the base, and another whose outputs
//   are in the range [0, base). The 2nd process is often but not always
//   trivial.
//
// FloatMult: The data is generated by a smooth distribution
//   whose outputs get multiplied by the base and perturbed by floating point
//   errors.
//
// FloatQuant: The data is generated by first drawing from a smooth distribution
//   on low-precision floats, then widening the result by adding
//   less-significant bits drawn from a second, very low-entropy distribution
//   (e.g. in the common case, one that always produces zeros).
//
// Note the differences between int mult and float mult,
// which have equivalent formulas.

/// A variation of how pco serializes and deserializes numbers.
///
/// Each mode splits the vector of numbers into one or two vectors of latents,
/// with a different formula for how the split and join is done.
/// We have delibrately written the formulas below in a slightly wrong way to
/// convey the correct intuition without dealing with implementation
/// complexities.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Mode<L: Latent> {
  /// Represents each number as a single latent: itself.
  ///
  /// Formula: `num = num`
  #[default]
  Classic,
  /// Given a `base`, represents each number as two latents: a multiplier
  /// on the base and an adjustment.
  ///
  /// Only applies to integers.
  ///
  /// Formula: `num = mode.base * mult + adjustment`
  IntMult(L),
  /// Given a float `base`, represents each number as two latents: a
  /// multiplier on the base and an ULPs (units-in-the-last-place) adjustment.
  ///
  /// Only applies to floats.
  ///
  /// Formula: `num = mode.base * mult + adjustment ULPs`
  FloatMult(L),
  /// Given a number of bits `k`, represents each number as two latents:
  /// quantums (effectively the first `TYPE_SIZE - k` bits) and an ULPs
  /// adjustment.
  ///
  /// Only applies to floats.
  ///
  /// Formula: `num = from_bits(quantums << k) + adjustment ULPs`
  /// (warning: this formula is especially simplified)
  FloatQuant(Bitlen),
}

impl<L: Latent> Mode<L> {
  pub(crate) fn n_latent_vars(&self) -> usize {
    use Mode::*;

    match self {
      Classic => 1,
      FloatMult(_) | IntMult(_) => 2, // multiplier, adjustment
      FloatQuant(_) => 2,             // quantums, adjustment
    }
  }

  pub(crate) fn delta_order_for_latent_var(
    &self,
    latent_var_idx: usize,
    delta_order: usize,
  ) -> usize {
    use Mode::*;

    match (self, latent_var_idx) {
      // In all currently-available modes, the overall `delta_order` is really the delta-order of
      // the first latent.
      (Classic, 0) | (FloatMult(_), 0) | (FloatQuant(_), 0) | (IntMult(_), 0) => delta_order,
      // In FloatMult, IntMult, and FloatQuant, the second latent is essentially a remainder or
      // adjustment; there isn't any a priori reason that deltas should be useful for that kind of
      // term and we do not attempt them.
      (FloatMult(_), 1) | (IntMult(_), 1) | (FloatQuant(_), 1) => 0,
      _ => unreachable!(
        "unknown latent {:?}/{}",
        self, latent_var_idx
      ),
    }
  }

  pub(crate) fn float_mult<F: FloatLike<L = L>>(base: F) -> Self {
    Self::FloatMult(base.to_latent_ordered())
  }
}
