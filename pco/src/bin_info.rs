use crate::ans::Symbol;
use crate::constants::{Bitlen, Weight};
use crate::data_types::Latent;
use crate::metadata::chunk_meta::Bin;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BinCompressionInfo<L: Latent> {
  // weight and upper are only used up through bin optimization, not dissection or writing
  pub weight: Weight,
  pub lower: L,
  pub upper: L,
  pub offset_bits: Bitlen,
  // symbol is also the index of this in the list of optimized compression infos
  pub symbol: Symbol,
}

impl<L: Latent> Default for BinCompressionInfo<L> {
  fn default() -> Self {
    BinCompressionInfo {
      weight: 0,
      lower: L::ZERO,
      upper: L::MAX,
      offset_bits: L::BITS,
      symbol: Symbol::MAX,
    }
  }
}

// Default here is meaningless and should only be used to fill in empty
// vectors.
#[derive(Clone, Copy, Debug, Default)]
pub struct BinDecompressionInfo<L: Latent> {
  pub lower: L,
  pub offset_bits: Bitlen,
}

impl<L: Latent> From<&Bin<L>> for BinDecompressionInfo<L> {
  fn from(bin: &Bin<L>) -> Self {
    Self {
      lower: bin.lower,
      offset_bits: bin.offset_bits,
    }
  }
}
