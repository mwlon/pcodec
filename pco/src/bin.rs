use crate::ans::Token;
use crate::constants::{Bitlen, Weight};
use crate::data_types::Latent;

/// Part of [`ChunkLatentVarMeta`][`crate::ChunkLatentVarMeta`] representing
/// a numerical range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Bin<U: Latent> {
  /// The number of occurrences of this bin in the asymmetric numeral system
  /// table.
  pub weight: Weight,
  /// The lower bound for this bin's numerical range.
  pub lower: U,
  /// The log of the size of this bin's (inclusive) numerical range.
  pub offset_bits: Bitlen,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BinCompressionInfo<U: Latent> {
  // weight and upper are only used up through bin optimization, not dissection or writing
  pub weight: Weight,
  pub lower: U,
  pub upper: U,
  pub offset_bits: Bitlen,
  // token is also the index of this in the list of optimized compression infos
  pub token: Token,
}

impl<U: Latent> From<BinCompressionInfo<U>> for Bin<U> {
  fn from(info: BinCompressionInfo<U>) -> Self {
    Bin {
      weight: info.weight,
      lower: info.lower,
      offset_bits: info.offset_bits,
    }
  }
}

impl<U: Latent> Default for BinCompressionInfo<U> {
  fn default() -> Self {
    BinCompressionInfo {
      weight: 0,
      lower: U::ZERO,
      upper: U::MAX,
      offset_bits: U::BITS,
      token: Token::MAX,
    }
  }
}

// Default here is meaningless and should only be used to fill in empty
// vectors.
#[derive(Clone, Copy, Debug, Default)]
pub struct BinDecompressionInfo<U: Latent> {
  pub lower: U,
  pub offset_bits: Bitlen,
}

impl<U: Latent> From<&Bin<U>> for BinDecompressionInfo<U> {
  fn from(bin: &Bin<U>) -> Self {
    Self {
      lower: bin.lower,
      offset_bits: bin.offset_bits,
    }
  }
}
