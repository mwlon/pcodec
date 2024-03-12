use crate::ans::Token;
use crate::constants::{Bitlen, Weight};
use crate::data_types::Latent;

/// Part of [`ChunkLatentVarMeta`][`crate::ChunkLatentVarMeta`] representing
/// a numerical range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Bin<L: Latent> {
  /// The number of occurrences of this bin in the asymmetric numeral system
  /// table.
  pub weight: Weight,
  /// The lower bound for this bin's numerical range.
  pub lower: L,
  /// The log of the size of this bin's (inclusive) numerical range.
  pub offset_bits: Bitlen,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BinCompressionInfo<L: Latent> {
  // weight and upper are only used up through bin optimization, not dissection or writing
  pub weight: Weight,
  pub lower: L,
  pub upper: L,
  pub offset_bits: Bitlen,
  // token is also the index of this in the list of optimized compression infos
  pub token: Token,
}

impl<L: Latent> From<BinCompressionInfo<L>> for Bin<L> {
  fn from(info: BinCompressionInfo<L>) -> Self {
    Bin {
      weight: info.weight,
      lower: info.lower,
      offset_bits: info.offset_bits,
    }
  }
}

impl<L: Latent> Default for BinCompressionInfo<L> {
  fn default() -> Self {
    BinCompressionInfo {
      weight: 0,
      lower: L::ZERO,
      upper: L::MAX,
      offset_bits: L::BITS,
      token: Token::MAX,
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
