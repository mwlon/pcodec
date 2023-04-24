use std::fmt::{Display, Formatter};

use crate::bits;
use crate::data_types::{NumberLike, UnsignedLike};

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
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Bin<T: NumberLike> {
  /// The count of numbers in the chunk that fall into this bin's range.
  /// Not available in wrapped mode.
  pub count: usize,
  /// The Huffman code for this bin. Collectively, all the bins for a
  /// chunk form a binary search tree (BST) over these Huffman codes.
  /// The BST over Huffman codes is different from the BST over numerical
  /// ranges.
  pub code: Vec<bool>,
  /// The lower bound for this bin's numerical range.
  pub lower: T,
  /// The upper bound (inclusive) for this bin's numerical range.
  pub upper: T,
  /// A parameter used for the most common bin in a sparse distribution.
  /// For instance, if 90% of a chunk's numbers are exactly 7, then the
  /// bin for the range `[7, 7]` will have a `run_len_jumpstart`.
  /// The jumpstart value tunes the varint encoding of the number of
  /// consecutive repetitions of the bin.
  pub run_len_jumpstart: Option<usize>,
  /// The greatest common divisor of all numbers belonging to this bin
  /// (in the data type's corresponding unsigned integer).
  pub gcd: T::Unsigned,
}

impl<T: NumberLike> Display for Bin<T> {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    let jumpstart_str = if let Some(jumpstart) = self.run_len_jumpstart {
      format!(" (jumpstart: {})", jumpstart)
    } else {
      "".to_string()
    };
    let gcd_str = if self.gcd > T::Unsigned::ONE {
      format!(" (gcd: {})", self.gcd)
    } else {
      "".to_string()
    };
    write!(
      f,
      "count: {} code: {} lower: {} upper: {}{}{}",
      self.count,
      bits::bits_to_string(&self.code),
      self.lower,
      self.upper,
      jumpstart_str,
      gcd_str,
    )
  }
}

// k is used internally to describe the number of bits
// required to describe an offset; k = ceil(log_2(upper - lower)).
impl<T: NumberLike> Bin<T> {
  pub(crate) fn k_info(&self) -> usize {
    let diff = (self.upper.to_unsigned() - self.lower.to_unsigned()) / self.gcd;
    if diff == T::Unsigned::ZERO {
      0
    } else {
      T::Unsigned::BITS - diff.leading_zeros()
    }
  }
}

// used during compression to determine Huffman codes
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WeightedPrefix<T: NumberLike> {
  pub bin: Bin<T>,
  // How to weight this bin during huffman coding,
  // in contrast to bin.count, which is the actual number of training
  // entries belonging to it.
  // Usually these are the same, but a bin with repetitions will have lower
  // weight than count.
  pub weight: usize,
}

impl<T: NumberLike> WeightedPrefix<T> {
  pub fn new(
    count: usize,
    weight: usize,
    lower: T,
    upper: T,
    run_len_jumpstart: Option<usize>,
    gcd: T::Unsigned,
  ) -> WeightedPrefix<T> {
    let bin = Bin {
      count,
      lower,
      upper,
      code: Vec::new(),
      run_len_jumpstart,
      gcd,
    };
    WeightedPrefix { bin, weight }
  }
}

#[derive(Clone, Copy, Debug)]
pub struct BinCompressionInfo<U: UnsignedLike> {
  pub count: usize,
  pub code: usize,
  pub code_len: usize,
  pub lower: U,
  pub upper: U,
  pub k: usize,
  pub run_len_jumpstart: Option<usize>,
  pub gcd: U,
}

impl<T: NumberLike> From<&Bin<T>> for BinCompressionInfo<T::Unsigned> {
  fn from(bin: &Bin<T>) -> Self {
    let k = bin.k_info();
    let code = bits::bits_to_usize(&bin.code);

    BinCompressionInfo {
      count: bin.count,
      code,
      code_len: bin.code.len(),
      lower: bin.lower.to_unsigned(),
      upper: bin.upper.to_unsigned(),
      k,
      run_len_jumpstart: bin.run_len_jumpstart,
      gcd: bin.gcd,
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
      k: U::BITS,
      run_len_jumpstart: None,
      gcd: U::ONE,
    }
  }
}

#[derive(Clone, Copy, Debug)]
pub struct BinDecompressionInfo<U: UnsignedLike> {
  pub lower_unsigned: U,
  pub k: usize,
  pub depth: usize,
  pub run_len_jumpstart: Option<usize>,
  pub gcd: U,
}

impl<U: UnsignedLike> Default for BinDecompressionInfo<U> {
  fn default() -> Self {
    BinDecompressionInfo {
      lower_unsigned: U::ZERO,
      k: U::BITS,
      depth: 0,
      run_len_jumpstart: None,
      gcd: U::ONE,
    }
  }
}

impl<T: NumberLike> From<&Bin<T>> for BinDecompressionInfo<T::Unsigned> {
  fn from(p: &Bin<T>) -> Self {
    let lower_unsigned = p.lower.to_unsigned();
    let k = p.k_info();
    BinDecompressionInfo {
      lower_unsigned,
      k,
      run_len_jumpstart: p.run_len_jumpstart,
      depth: p.code.len(),
      gcd: p.gcd,
    }
  }
}
