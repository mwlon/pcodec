use crate::bin::BinCompressionInfo;

use crate::constants::Weight;
use crate::data_types::UnsignedLike;

use crate::modes::ConstMode;
use crate::{bits, Bin};

// formula: bin lower + offset * bin gcd
#[derive(Clone, Copy, Debug)]
pub struct GcdMode;

#[derive(Default)]
pub struct OptAccumulator<U: UnsignedLike> {
  upper: Option<U>,
  gcd: Option<U>,
}

impl<U: UnsignedLike> ConstMode<U> for GcdMode {
  type BinOptAccumulator = OptAccumulator<U>;
  fn combine_bin_opt_acc(bin: &BinCompressionInfo<U>, acc: &mut Self::BinOptAccumulator) {
    // folding GCD's involves GCD'ing with their modulo offset and (if the new
    // range is nontrivial) with the new bin's GCD
    if let Some(upper) = acc.upper {
      acc.gcd = Some(match acc.gcd {
        Some(gcd) => pair_gcd(upper - bin.upper, gcd),
        None => upper - bin.upper,
      });
    } else {
      acc.upper = Some(bin.upper);
    }

    if bin.upper != bin.lower {
      acc.gcd = Some(match acc.gcd {
        Some(gcd) => pair_gcd(bin.gcd, gcd),
        None => bin.gcd,
      });
    }
  }

  fn bin_cost(&self, lower: U, upper: U, count: Weight, acc: &Self::BinOptAccumulator) -> f64 {
    // best approximation of GCD metadata bit cost we can do without knowing
    // what's going on in the other bins
    let bin_gcd = acc.gcd.unwrap_or(U::ONE);
    let gcd_meta_cost = if bin_gcd > U::ONE {
      U::BITS as f64
    } else {
      0.0
    };
    let offset_cost = bits::bits_to_encode_offset((upper - lower) / bin_gcd);
    gcd_meta_cost + (offset_cost as u64 * count as u64) as f64
  }

  fn fill_optimized_compression_info(
    &self,
    acc: Self::BinOptAccumulator,
    bin: &mut BinCompressionInfo<U>,
  ) {
    let gcd = acc.gcd.unwrap_or(U::ONE);
    let max_offset = (bin.upper - bin.lower) / gcd;
    bin.gcd = gcd;
    bin.offset_bits = bits::bits_to_encode_offset(max_offset);
  }
}

// fast if b is small, requires b > 0
pub fn pair_gcd<U: UnsignedLike>(mut a: U, mut b: U) -> U {
  loop {
    a %= b;
    if a == U::ZERO {
      return b;
    }
    b %= a;
    if b == U::ZERO {
      return a;
    }
  }
}

pub fn gcd<U: UnsignedLike>(sorted: &[U]) -> U {
  let lower = sorted[0];
  let upper = sorted[sorted.len() - 1];
  if lower == upper {
    return U::ONE;
  }
  let mut res = upper - lower;
  for &x in sorted.iter().skip(1) {
    if res == U::ONE {
      break;
    }
    res = pair_gcd(x - lower, res);
  }
  res
}

pub fn use_gcd_bin_optimize<U: UnsignedLike>(bins: &[BinCompressionInfo<U>]) -> bool {
  for p in bins {
    if p.gcd > U::ONE {
      return true;
    }
  }
  for (i, pi) in bins.iter().enumerate().skip(1) {
    let pj = &bins[i - 1];
    if pi.offset_bits == 0 && pj.offset_bits == 0 && pj.lower + U::ONE < pi.lower {
      return true;
    }
  }
  false
}

pub fn use_gcd_arithmetic<U: UnsignedLike>(bins: &[Bin<U>]) -> bool {
  bins.iter().any(|p| p.gcd > U::ONE && p.offset_bits > 0)
}

#[cfg(test)]
mod tests {
  use crate::modes::gcd::*;

  #[test]
  fn test_pair_gcd() {
    assert_eq!(pair_gcd(0_u32, 14), 14);
    assert_eq!(pair_gcd(7_u32, 14), 7);
    assert_eq!(pair_gcd(8_u32, 14), 2);
    assert_eq!(pair_gcd(9_u32, 14), 1);
    assert_eq!(pair_gcd(8_u32, 20), 4);
    assert_eq!(pair_gcd(1_u32, 6), 1);
    assert_eq!(pair_gcd(6_u32, 1), 1);
    assert_eq!(pair_gcd(7, u64::MAX), 1);
    assert_eq!(pair_gcd(7, (1_u64 << 63) - 1), 7);
  }

  #[test]
  fn test_gcd() {
    assert_eq!(gcd(&[0_u32, 4, 6, 8, 10]), 2);
    assert_eq!(gcd(&[0_u32, 4, 6, 8, 10, 11]), 1);
  }
}
