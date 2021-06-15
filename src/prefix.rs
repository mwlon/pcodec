use std::fmt::{Display, Formatter};
use std::fmt;

use crate::bits;
use crate::types::NumberLike;

#[derive(Clone, Copy, Debug, Default)]
pub struct PrefixDecompressionInfo<T> where T: NumberLike {
  pub lower: T,
  pub upper: T,
  pub k: u32,
  pub run_len_jumpstart: Option<usize>,
}

impl<T> PrefixDecompressionInfo<T> where T: NumberLike {
  pub fn new() -> Self {
    PrefixDecompressionInfo {
      ..Default::default()
    }
  }
}

impl<T> From<&Prefix<T>> for PrefixDecompressionInfo<T> where T: NumberLike {
  fn from(p: &Prefix<T>) -> Self {
    PrefixDecompressionInfo {
      lower: p.lower,
      upper: p.upper,
      k: p.k,
      run_len_jumpstart: p.run_len_jumpstart,
    }
  }
}

#[derive(Clone, Debug)]
pub struct Prefix<T> where T: NumberLike {
  pub val: Vec<bool>,
  pub lower: T,
  pub upper: T,
  pub k: u32,
  pub only_k_bits_lower: u64,
  pub only_k_bits_upper: u64,
  pub run_len_jumpstart: Option<usize>,
}

// In Prefix and PrefixIntermediate, lower and upper are always inclusive.
// This allows handling extremal values.
impl<T> Prefix<T> where T: NumberLike {
  pub fn from_intermediate_and_diff(intermediate: &PrefixIntermediate<T>, diff: u64) -> Prefix<T> {
    Self::new(
      intermediate.val.clone(),
      intermediate.lower,
      intermediate.upper,
      diff,
      intermediate.run_len_jumpstart,
    )
  }

  pub fn new(val: Vec<bool>, lower: T, upper: T, diff: u64, run_len_jumpstart: Option<usize>) -> Prefix<T> {
    let k = ((diff as f64) + 1.0).log2().floor() as u32;
    let only_k_bits_upper = if k == 64 {
      u64::MAX
    } else {
      (1_u64 << k) - 1
    };
    let only_k_bits_lower = diff - only_k_bits_upper;

    Prefix {
      val,
      lower,
      upper,
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

#[derive(Clone, Debug)]
pub struct PrefixIntermediate<T> {
  pub weight: u64,
  pub lower: T,
  pub upper: T,
  pub val: Vec<bool>,
  pub run_len_jumpstart: Option<usize>,
}

impl<T> PrefixIntermediate<T> {
  pub fn new(weight: u64, lower: T, upper: T, run_len_jumpstart: Option<usize>) -> PrefixIntermediate<T> {
    PrefixIntermediate {
      weight,
      lower,
      upper,
      val: Vec::new(),
      run_len_jumpstart,
    }
  }
}
