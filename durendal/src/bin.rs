use std::fmt::{Display, Formatter};

use crate::bits;
use crate::constants::Bitlen;
use crate::data_types::UnsignedLike;
use crate::modes::ModeBin;

/// A pairing of a Huffman code with a numerical range.
///
/// Quantile Compression works by splitting the distribution of numbers
/// into ranges and associating a Huffman code (a short sequence of bits)
/// with each range.
/// The combination of these pieces of information, plus a couple others,
/// is called a `Bin`.
/// When compressing a number, the compressor finds the bin containing
/// it, then writes out its Huffman code, optionally the number of
/// consecutive repetitions of that number if `run_length_jumpstart` is
/// available, and then the exact offset within the range for the number.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Bin<U: UnsignedLike> {
  /// The count of numbers in the chunk that fall into this bin's range.
  /// Not available in wrapped mode.
  pub count: usize,
  /// The Huffman code for this bin. Collectively, all the bins for a
  /// chunk form a binary search tree (BST) over these Huffman codes.
  /// The BST over Huffman codes is different from the BST over numerical
  /// ranges.
  pub code: usize,
  pub code_len: Bitlen,
  /// A parameter used for the most common bin in a sparse distribution.
  /// For instance, if 90% of a chunk's numbers are exactly 7, then the
  /// bin for the range `[7, 7]` will have a `run_len_jumpstart`.
  /// The jumpstart value tunes the varint encoding of the number of
  /// consecutive repetitions of the bin.
  pub run_len_jumpstart: Option<Bitlen>,
  /// The lower bound for this bin's numerical range.
  pub lower: U,
  /// The log of the size of this bin's (inclusive) numerical range.
  pub offset_bits: Bitlen,
  /// The greatest common divisor of all numbers belonging to this bin
  /// (in the data type's corresponding unsigned integer).
  pub gcd: U,
  /// TODO
  pub float_mult_base: U::Float,
  pub adj_bits: Bitlen,
}

impl<U: UnsignedLike> Display for Bin<U> {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    let jumpstart_str = if let Some(jumpstart) = self.run_len_jumpstart {
      format!(" (jumpstart: {})", jumpstart)
    } else {
      "".to_string()
    };
    let gcd_str = if self.gcd > U::ONE {
      format!(" (gcd: {})", self.gcd)
    } else {
      "".to_string()
    };
    let code_str = bits::code_to_string(self.code, self.code_len);
    write!(
      f,
      "count: {} code: {} lower: {} offset bits: {}{}{}",
      self.count, code_str, self.lower, self.offset_bits, jumpstart_str, gcd_str,
    )
  }
}

impl<U: UnsignedLike> BinCompressionInfo<U> {
  pub fn new(count: usize, lower: U, upper: U, run_len_jumpstart: Option<Bitlen>, gcd: U) -> Self {
    let diff = (upper - lower) / gcd;
    let offset_bits = if diff == U::ZERO {
      0
    } else {
      U::BITS - diff.leading_zeros()
    };
    BinCompressionInfo {
      count,
      lower,
      upper,
      offset_bits,
      gcd,
      run_len_jumpstart,
      code: 0,
      code_len: 0,
      float_mult_lower: U::Float::default(),
      adj_lower: U::ZERO,
      adj_bits: 0,
    }
  }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BinCompressionInfo<U: UnsignedLike> {
  pub count: usize,
  pub code: usize,
  pub code_len: Bitlen,
  pub lower: U,
  pub upper: U,
  pub offset_bits: Bitlen,
  pub run_len_jumpstart: Option<Bitlen>,
  pub gcd: U,
  pub float_mult_lower: U::Float,
  pub adj_lower: U,
  pub adj_bits: Bitlen,
}

impl<U: UnsignedLike> From<BinCompressionInfo<U>> for Bin<U> {
  fn from(info: BinCompressionInfo<U>) -> Self {
    Bin {
      count: info.count,
      code: info.code,
      code_len: info.code_len,
      lower: info.lower,
      offset_bits: info.offset_bits,
      run_len_jumpstart: info.run_len_jumpstart,
      gcd: info.gcd,
      float_mult_base: info.float_mult_lower,
      adj_bits: info.adj_bits,
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
      count: 0,
      code: 0,
      code_len: 0,
      lower: U::ZERO,
      upper: U::MAX,
      offset_bits: U::BITS,
      run_len_jumpstart: None,
      gcd: U::ONE,
      float_mult_lower: U::Float::default(),
      adj_lower: U::ZERO,
      adj_bits: 0,
    }
  }
}

// Default here is meaningless and should only be used to fill in empty
// vectors.
// Note that different modes use the unsigneds and bitlens in different ways,
// so we given them lousy names. This is a hack but probably better than adding
// a bunch more generics and associated types.
#[derive(Clone, Copy, Debug, Default)]
pub struct BinDecompressionInfo<B: ModeBin> {
  pub depth: Bitlen,
  pub run_len_jumpstart: Option<Bitlen>,
  pub mode_bin: B,
}
