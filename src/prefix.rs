use crate::utils::u64_diff;

#[derive(Clone)]
pub struct Prefix {
  pub val: Vec<bool>,
  pub lower: i64,
  pub upper: i64,
  pub k: u32,
  pub only_k_bits_lower: u64,
  pub only_k_bits_upper: u64,
}

// In Prefix and PrefixIntermediate, lower and upper are always inclusive.
// This allows handling extremal values.
impl Prefix {
  pub fn new(val: Vec<bool>, lower: i64, upper: i64) -> Prefix {
    let size = u64_diff(upper, lower);
    let k = ((size as f64) + 1.0).log2().floor() as u32;
    let only_k_bits_upper = if k == 64 {
      u64::MAX
    } else {
      (1_u64 << k) - 1
    };
    let only_k_bits_lower = size - only_k_bits_upper;

    return Prefix {
      val,
      lower,
      upper,
      k,
      only_k_bits_lower,
      only_k_bits_upper,
    }
  }
}

pub struct PrefixIntermediate {
  pub weight: u64,
  pub lower: i64,
  pub upper: i64,
  pub val: Vec<bool>,
}

impl PrefixIntermediate {
  pub fn new(weight: u64, min: i64, max: i64) -> PrefixIntermediate {
    return PrefixIntermediate {
      weight,
      lower: min,
      upper: max,
      val: Vec::new(),
    };
  }
}
