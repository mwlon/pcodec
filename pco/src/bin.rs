use crate::ans::Token;
use crate::constants::Bitlen;
use crate::data_types::UnsignedLike;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Bin<U: UnsignedLike> {
  /// The number of occurrences of this bin in the asymmetric numeral system
  /// table.
  pub weight: usize,
  /// The lower bound for this bin's numerical range.
  pub lower: U,
  /// The log of the size of this bin's (inclusive) numerical range.
  pub offset_bits: Bitlen,
  /// The greatest common divisor of all numbers belonging to this bin
  /// (in the data type's corresponding unsigned integer).
  pub gcd: U,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BinCompressionInfo<U: UnsignedLike> {
  pub weight: usize,
  pub lower: U,
  pub upper: U,
  pub offset_bits: Bitlen,
  pub gcd: U,
  // token is also the index of this in the list of optimized compression infos
  pub token: Token,
}

impl<U: UnsignedLike> From<BinCompressionInfo<U>> for Bin<U> {
  fn from(info: BinCompressionInfo<U>) -> Self {
    Bin {
      weight: info.weight,
      lower: info.lower,
      offset_bits: info.offset_bits,
      gcd: info.gcd,
    }
  }
}

impl<U: UnsignedLike> BinCompressionInfo<U> {
  pub fn contains(&self, unsigned: U) -> bool {
    self.lower <= unsigned && self.upper >= unsigned
  }
}

impl<U: UnsignedLike> Default for BinCompressionInfo<U> {
  fn default() -> Self {
    BinCompressionInfo {
      weight: 0,
      lower: U::ZERO,
      upper: U::MAX,
      offset_bits: U::BITS,
      gcd: U::ONE,
      token: Token::MAX,
    }
  }
}

// Default here is meaningless and should only be used to fill in empty
// vectors.
#[derive(Clone, Copy, Debug, Default)]
pub struct BinDecompressionInfo<U: UnsignedLike> {
  pub lower: U,
  pub offset_bits: Bitlen,
  pub gcd: U,
}

impl<U: UnsignedLike> From<&Bin<U>> for BinDecompressionInfo<U> {
  fn from(bin: &Bin<U>) -> Self {
    Self {
      lower: bin.lower,
      offset_bits: bin.offset_bits,
      gcd: bin.gcd,
    }
  }
}
