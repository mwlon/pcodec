use std::fmt::{Display, Formatter};
use crate::bits;
use crate::data_types::{NumberLike, UnsignedLike};

/// A pairing of a Huffman code with a numerical range.
///
/// Quantile Compression works by splitting the distribution of numbers
/// into ranges and associating a Huffman code (a short sequence of bits)
/// with each range.
/// The combination of these pieces of information, plus a couple others,
/// is called a `Prefix`.
/// When compressing a number, the compressor finds the prefix containing
/// it, then writes out its Huffman code, optionally the number of
/// consecutive repetitions of that number if `run_length_jumpstart` is
/// available, and then the exact offset within the range for the number.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Prefix<T> where T: NumberLike {
  /// The count of numbers in the chunk that fall into this Prefix's range.
  pub count: usize,
  /// The Huffman code for this prefix. Collectively, all the prefixes for a
  /// chunk form a binary search tree (BST) over these Huffman codes.
  /// The BST over Huffman codes is different from the BST over numerical
  /// ranges.
  pub code: Vec<bool>,
  /// The lower bound for this prefix's numerical range.
  pub lower: T,
  /// The upper bound (inclusive) for this prefix's numerical range.
  pub upper: T,
  /// A parameter used for the most common prefix in a sparse distribution.
  /// For instance, if 90% of a chunk's numbers are exactly 77, then the
  /// prefix for the range `[0, 0]` will have a `run_len_jumpstart`.
  /// The jumpstart value tunes the varint encoding of the number of
  /// consecutive repetitions of the prefix.
  pub run_len_jumpstart: Option<usize>,
}

impl<T: NumberLike> Display for Prefix<T> {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    let jumpstart_str = if let Some(jumpstart) = self.run_len_jumpstart {
      format!("(jumpstart: {})", jumpstart)
    } else {
      "".to_string()
    };
    write!(
      f,
      "count: {} code: {} lower: {} upper: {} {}",
      self.count,
      bits::bits_to_string(&self.code),
      self.lower,
      self.upper,
      jumpstart_str,
    )
  }
}

// k is used internally to describe the minimum number of bits
// required to describe an offset; k = floor(log_2(upper - lower)).
// Each offset is encoded as k bit if it is between
// only_k_bits_lower and only_k_bits_upper, or
// or k + 1 bits otherwise.
pub(crate) struct KInfo<T: NumberLike> {
  pub k: usize,
  pub only_k_bits_lower: T::Unsigned,
  pub only_k_bits_upper: T::Unsigned,
}

impl<T: NumberLike> Prefix<T> {
  pub(crate) fn k_info(&self) -> KInfo<T> {
    let lower_unsigned = self.lower.to_unsigned();
    let diff = self.upper.to_unsigned() - lower_unsigned;
    let k = (diff.to_f64() + 1.0).log2().floor() as usize;
    let only_k_bits_upper = if k == T::Unsigned::BITS {
      T::Unsigned::MAX
    } else {
      (T::Unsigned::ONE << k) - T::Unsigned::ONE
    };
    let only_k_bits_lower = diff - only_k_bits_upper;

    KInfo {
      k,
      only_k_bits_lower,
      only_k_bits_upper,
    }
  }
}

// used during compression to determine Huffman codes
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WeightedPrefix<T: NumberLike> {
  pub prefix: Prefix<T>,
  // How to weight this prefix during huffman coding,
  // in contrast to prefix.count, which is the actual number of training
  // entries belonging to it.
  // Usually these are the same, but a prefix with repetitions will have lower
  // weight than count.
  pub weight: usize,
}

impl<T: NumberLike> WeightedPrefix<T> {
  pub fn new(count: usize, weight: usize, lower: T, upper: T, run_len_jumpstart: Option<usize>) -> WeightedPrefix<T> {
    let prefix = Prefix {
      count,
      lower,
      upper,
      code: Vec::new(),
      run_len_jumpstart
    };
    WeightedPrefix {
      prefix,
      weight,
    }
  }
}

#[derive(Clone, Copy, Debug)]
pub struct PrefixCompressionInfo<Diff> where Diff: UnsignedLike {
  pub count: usize,
  pub code: usize,
  pub code_len: usize,
  pub lower: Diff,
  pub upper: Diff,
  pub k: usize,
  pub only_k_bits_lower: Diff,
  pub only_k_bits_upper: Diff,
  pub run_len_jumpstart: Option<usize>,
}

impl<T: NumberLike> From<&Prefix<T>> for PrefixCompressionInfo<T::Unsigned> {
  fn from(prefix: &Prefix<T>) -> Self {
    let KInfo { k, only_k_bits_upper, only_k_bits_lower } = prefix.k_info();
    let code = bits::bits_to_usize(&prefix.code);

    PrefixCompressionInfo {
      count: prefix.count,
      code,
      code_len: prefix.code.len(),
      lower: prefix.lower.to_unsigned(),
      upper: prefix.upper.to_unsigned(),
      k,
      only_k_bits_lower,
      only_k_bits_upper,
      run_len_jumpstart: prefix.run_len_jumpstart,
    }
  }
}

impl<Diff: UnsignedLike> PrefixCompressionInfo<Diff> {
  pub fn contains(&self, unsigned: Diff) -> bool {
    self.lower <= unsigned && self.upper >= unsigned
  }
}

impl<Diff: UnsignedLike> Default for PrefixCompressionInfo<Diff> {
  fn default() -> Self {
    PrefixCompressionInfo {
      count: 0,
      code: 0,
      code_len: 0,
      lower: Diff::ZERO,
      upper: Diff::MAX,
      k: Diff::BITS,
      only_k_bits_lower: Diff::ZERO,
      only_k_bits_upper: Diff::MAX,
      run_len_jumpstart: None,
    }
  }
}

#[derive(Clone, Copy, Debug)]
pub struct PrefixDecompressionInfo<Diff> where Diff: UnsignedLike {
  pub lower_unsigned: Diff,
  pub range: Diff,
  pub k: usize,
  pub depth: usize,
  pub run_len_jumpstart: Option<usize>,
  pub most_significant: Diff,
}

impl<Diff: UnsignedLike> Default for PrefixDecompressionInfo<Diff> {
  fn default() -> Self {
    PrefixDecompressionInfo {
      lower_unsigned: Diff::ZERO,
      range: Diff::MAX,
      k: Diff::BITS,
      depth: 0,
      run_len_jumpstart: None,
      most_significant: Diff::ZERO,
    }
  }
}

impl<T> From<&Prefix<T>> for PrefixDecompressionInfo<T::Unsigned> where T: NumberLike {
  fn from(p: &Prefix<T>) -> Self {
    let lower_unsigned = p.lower.to_unsigned();
    let upper_unsigned = p.upper.to_unsigned();
    let KInfo { k, only_k_bits_lower: _, only_k_bits_upper: _ } = p.k_info();
    let most_significant = if k == T::PHYSICAL_BITS {
      T::Unsigned::ZERO
    } else {
      T::Unsigned::ONE << k
    };
    PrefixDecompressionInfo {
      lower_unsigned,
      range: upper_unsigned - lower_unsigned,
      k,
      run_len_jumpstart: p.run_len_jumpstart,
      depth: p.code.len(),
      most_significant,
    }
  }
}
