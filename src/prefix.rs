use crate::bits::u64_diff;

#[derive(Clone)]
pub struct Prefix {
  pub val: Vec<bool>,
  pub lower: i64,
  pub upper: i64,
  pub k: u32,
  pub km1min: u64,
  pub km1max: u64,
}

impl Prefix {
  pub fn new(val: Vec<bool>, lower: i64, upper: i64) -> Prefix {
    let size = u64_diff(upper, lower);
    let k = (size as f64).log2().floor() as u32;
    let km1max = (1 as u64) << k;
    let km1min = size - km1max;

    return Prefix {
      val,
      lower,
      upper,
      k,
      km1min,
      km1max,
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
