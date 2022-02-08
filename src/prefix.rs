use std::fmt::{Display, Formatter};
use std::fmt;

use crate::bits;
use crate::data_types::{NumberLike, UnsignedLike};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Prefix<T> where T: NumberLike {
  pub count: usize,
  pub code: Vec<bool>,
  pub lower: T,
  pub upper: T,
  pub run_len_jumpstart: Option<usize>,
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

impl<T> Display for Prefix<T> where T: NumberLike {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    let reps_info = match self.run_len_jumpstart {
      None => "".to_string(),
      Some(jumpstart) => format!(" (>={} run length bits)", jumpstart)
    };

    write!(
      f,
      "{}: {} to {}{}",
      bits::bits_to_string(&self.code),
      self.lower,
      self.upper,
      reps_info,
    )
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
  pub weight: u64,
}

impl<T: NumberLike> WeightedPrefix<T> {
  pub fn new(count: usize, weight: u64, lower: T, upper: T, run_len_jumpstart: Option<usize>) -> WeightedPrefix<T> {
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

#[derive(Clone, Debug)]
pub struct PrefixCompressionInfo<T> where T: NumberLike {
  pub count: usize,
  pub code: Vec<bool>,
  pub lower: T,
  pub upper: T,
  pub lower_unsigned: T::Unsigned,
  pub k: usize,
  pub only_k_bits_lower: T::Unsigned,
  pub only_k_bits_upper: T::Unsigned,
  pub run_len_jumpstart: Option<usize>,
}

impl<T: NumberLike> From<&Prefix<T>> for PrefixCompressionInfo<T> {
  fn from(prefix: &Prefix<T>) -> Self {
    let KInfo { k, only_k_bits_upper, only_k_bits_lower } = prefix.k_info();

    PrefixCompressionInfo {
      count: prefix.count,
      code: prefix.code.clone(),
      lower: prefix.lower,
      upper: prefix.upper,
      lower_unsigned: prefix.lower.to_unsigned(),
      k,
      only_k_bits_lower,
      only_k_bits_upper,
      run_len_jumpstart: prefix.run_len_jumpstart,
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
}

impl<Diff: UnsignedLike> Default for PrefixDecompressionInfo<Diff> {
  fn default() -> Self {
    PrefixDecompressionInfo {
      lower_unsigned: Diff::ZERO,
      range: Diff::MAX,
      k: Diff::BITS,
      depth: 0,
      run_len_jumpstart: None,
    }
  }
}

impl<T> From<&Prefix<T>> for PrefixDecompressionInfo<T::Unsigned> where T: NumberLike {
  fn from(p: &Prefix<T>) -> Self {
    let lower_unsigned = p.lower.to_unsigned();
    let upper_unsigned = p.upper.to_unsigned();
    let KInfo { k, only_k_bits_lower: _, only_k_bits_upper: _ } = p.k_info();
    PrefixDecompressionInfo {
      lower_unsigned,
      range: upper_unsigned - lower_unsigned,
      k,
      run_len_jumpstart: p.run_len_jumpstart,
      depth: p.code.len(),
    }
  }
}

pub fn display_prefixes<T: NumberLike>(prefixes: &[Prefix<T>], f: &mut fmt::Formatter<'_>) -> fmt::Result {
  let s = prefixes
    .iter()
    .map(|p| p.to_string())
    .collect::<Vec<String>>()
    .join("\n");
  write!(f, "({} prefixes)\n{}", prefixes.len(), s)
}
