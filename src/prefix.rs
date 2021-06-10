use std::fmt::{Display, Formatter};
use std::fmt;

use crate::bits;
use crate::types::NumberLike;

#[derive(Clone, Debug)]
pub struct Prefix<T> where T: NumberLike {
  pub val: Vec<bool>,
  pub lower: T,
  pub upper: T,
  pub k: u32,
  pub only_k_bits_lower: u64,
  pub only_k_bits_upper: u64,
  pub max_bits: usize,
  pub reps: usize,
}

// In Prefix and PrefixIntermediate, lower and upper are always inclusive.
// This allows handling extremal values.
impl<T> Prefix<T> where T: NumberLike {
  pub fn new(val: Vec<bool>, lower: T, upper: T, diff: u64, reps: usize) -> Prefix<T> {
    let k = ((diff as f64) + 1.0).log2().floor() as u32;
    let only_k_bits_upper = if k == 64 {
      u64::MAX
    } else {
      (1_u64 << k) - 1
    };
    let only_k_bits_lower = diff - only_k_bits_upper;
    let max_bits = val.len() + 1 + k as usize;

    Prefix {
      val,
      lower,
      upper,
      k,
      only_k_bits_lower,
      only_k_bits_upper,
      max_bits,
      reps,
    }
  }
}

impl<T> Display for Prefix<T> where T: NumberLike {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    let reps_info = if self.reps == 1 {
      "".to_string()
    } else {
      format!(" (x{})", self.reps)
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
  pub reps: usize,
}

impl<T> PrefixIntermediate<T> {
  pub fn new(weight: u64, lower: T, upper: T, reps: usize) -> PrefixIntermediate<T> {
    PrefixIntermediate {
      weight,
      lower,
      upper,
      val: Vec::new(),
      reps,
    }
  }
}
