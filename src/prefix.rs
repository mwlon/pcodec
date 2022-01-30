use std::fmt::{Display, Formatter};
use std::fmt;

use crate::bits;
use crate::types::{NumberLike, UnsignedLike};

#[derive(Clone, Copy, Debug)]
pub struct PrefixDecompressionInfo<Diff> where Diff: UnsignedLike {
  pub lower_unsigned: Diff,
  pub range: Diff,
  pub k: usize,
  pub depth: usize,
  pub run_len_jumpstart: Option<usize>,
}

impl<Diff> Default for PrefixDecompressionInfo<Diff> where Diff: UnsignedLike {
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
    PrefixDecompressionInfo {
      lower_unsigned,
      range: upper_unsigned - lower_unsigned,
      k: p.k,
      run_len_jumpstart: p.run_len_jumpstart,
      depth: p.val.len(),
    }
  }
}

#[derive(Clone, Debug)]
pub struct Prefix<T> where T: NumberLike {
  pub count: usize,
  pub val: Vec<bool>,
  pub lower: T,
  pub upper: T,
  pub lower_unsigned: T::Unsigned,
  pub k: usize,
  pub only_k_bits_lower: T::Unsigned,
  pub only_k_bits_upper: T::Unsigned,
  pub run_len_jumpstart: Option<usize>,
}

impl<T: NumberLike> From<PrefixIntermediate<T>> for Prefix<T> {
  fn from(intermediate: PrefixIntermediate<T>) -> Self {
    Self::new(
      intermediate.count,
      intermediate.val.clone(),
      intermediate.lower,
      intermediate.upper,
      intermediate.run_len_jumpstart,
    )
  }
}

// In Prefix and PrefixIntermediate, lower and upper are always inclusive.
// This allows handling extremal values.
impl<T> Prefix<T> where T: NumberLike {
  pub fn new(count: usize, val: Vec<bool>, lower: T, upper: T, run_len_jumpstart: Option<usize>) -> Prefix<T> {
    let lower_unsigned = lower.to_unsigned();
    let diff = upper.to_unsigned() - lower_unsigned;
    let k = (diff.to_f64() + 1.0).log2().floor() as usize;
    let only_k_bits_upper = if k == T::Unsigned::BITS {
      T::Unsigned::MAX
    } else {
      (T::Unsigned::ONE << k) - T::Unsigned::ONE
    };
    let only_k_bits_lower = diff - only_k_bits_upper;

    Prefix {
      count,
      val,
      lower,
      upper,
      lower_unsigned,
      k,
      only_k_bits_lower,
      only_k_bits_upper,
      run_len_jumpstart,
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
      bits::bits_to_string(&self.val),
      self.lower,
      self.upper,
      reps_info,
    )
  }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PrefixIntermediate<T: NumberLike> {
  pub count: usize, // the actual number of training entries belonging to this prefix
  pub weight: u64, // how to weight this prefix during huffman coding
  pub lower: T,
  pub upper: T,
  pub val: Vec<bool>,
  pub run_len_jumpstart: Option<usize>,
}

impl<T: NumberLike> PrefixIntermediate<T> {
  pub fn new(count: usize, weight: u64, lower: T, upper: T, run_len_jumpstart: Option<usize>) -> PrefixIntermediate<T> {
    PrefixIntermediate {
      count,
      weight,
      lower,
      upper,
      val: Vec::new(),
      run_len_jumpstart,
    }
  }
}
