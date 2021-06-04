use crate::types::NumberLike;

#[derive(Clone)]
pub struct Prefix<T> where T: NumberLike {
  pub val: Vec<bool>,
  pub lower: T,
  pub upper: T,
  pub k: u32,
  pub only_k_bits_lower: u64,
  pub only_k_bits_upper: u64,
  pub max_bits: usize,
}

// In Prefix and PrefixIntermediate, lower and upper are always inclusive.
// This allows handling extremal values.
impl<T> Prefix<T> where T: NumberLike {
  pub fn new(val: Vec<bool>, lower: T, upper: T, diff: u64) -> Prefix<T> {
    let k = ((diff as f64) + 1.0).log2().floor() as u32;
    let only_k_bits_upper = if k == 64 {
      u64::MAX
    } else {
      (1_u64 << k) - 1
    };
    let only_k_bits_lower = diff - only_k_bits_upper;
    let max_bits = val.len() + 1 + k as usize;

    return Prefix {
      val,
      lower,
      upper,
      k,
      only_k_bits_lower,
      only_k_bits_upper,
      max_bits,
    }
  }
}

pub struct PrefixIntermediate<T> {
  pub weight: u64,
  pub lower: T,
  pub upper: T,
  pub val: Vec<bool>,
}

impl<T> PrefixIntermediate<T> {
  pub fn new(weight: u64, lower: T, upper: T) -> PrefixIntermediate<T> {
    return PrefixIntermediate {
      weight,
      lower,
      upper,
      val: Vec::new(),
    };
  }
}
