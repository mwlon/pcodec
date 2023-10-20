use std::fmt::Debug;

use crate::bin::{BinCompressionInfo, BinDecompressionInfo};
use crate::bit_reader::BitReader;
use crate::constants::Weight;
use crate::data_types::UnsignedLike;
use crate::errors::PcoResult;
use crate::float_mult_utils::FloatMultConfig;

// Static, compile-time modes. Logic should go here if it's called in hot
// loops.
pub trait ConstMode<U: UnsignedLike>: Copy + Debug + 'static {
  // BIN OPTIMIZATION
  type BinOptAccumulator: Default;
  fn combine_bin_opt_acc(bin: &BinCompressionInfo<U>, acc: &mut Self::BinOptAccumulator);
  fn bin_cost(&self, lower: U, upper: U, count: Weight, acc: &Self::BinOptAccumulator) -> f64;
  fn fill_optimized_compression_info(
    &self,
    acc: Self::BinOptAccumulator,
    bin: &mut BinCompressionInfo<U>,
  );
}

// Dynamic modes. Logic should go here if it isn't called in hot loops.
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
  Gcd,
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
  pub(crate) fn n_latents(&self) -> usize {
    match self {
      Mode::Classic | Mode::Gcd => 1,
      Mode::FloatMult(_) => 2,
    }
  }

  pub(crate) fn latent_delta_order(&self, latent_idx: usize, delta_order: usize) -> usize {
    match (self, latent_idx) {
      (Mode::Classic, 0) | (Mode::Gcd, 0) | (Mode::FloatMult(_), 0) => delta_order,
      (Mode::FloatMult(_), 1) => 0,
      _ => panic!(
        "should be unreachable; unknown latent {:?}/{}",
        self, latent_idx
      ),
    }
  }
}
