use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::data_types::{NumberLike, UnsignedLike};
use crate::errors::{QCompressError, QCompressResult};
use crate::{Flags, Prefix};

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

// Returns Some(gcd) if it is more concise to use the same GCD for all prefixes
// Returns None if it is more concise to describe each prefix's GCD separately
// 4 cases:
// * no prefixes: we don't even need to bother writing a common GCD, return None
// * all prefixes have range 0, i.e. [x, x]: GCD doesn't affect num blocks, return Some(1)
// * all prefixes with range >0 have same GCD: return Some(that GCD)
// * two prefixes with range >0 have different GCD: return None
pub fn common_gcd_for_chunk_meta<T: NumberLike>(prefixes: &[Prefix<T>]) -> Option<T::Unsigned> {
  let mut nontrivial_ranges_share_gcd: bool = true;
  let mut gcd = None;
  for p in prefixes {
    if p.upper != p.lower {
      if gcd.is_none() {
        gcd = Some(p.gcd);
      } else if gcd != Some(p.gcd) {
        nontrivial_ranges_share_gcd = false;
      }
    }
  }

  match (
    prefixes.len(),
    nontrivial_ranges_share_gcd,
    gcd,
  ) {
    (0, _, _) => None,
    (_, false, _) => None,
    (_, true, Some(gcd)) => Some(gcd),
    (_, _, None) => Some(T::Unsigned::ONE),
  }
}

pub fn use_gcd_prefix_optimize<T: NumberLike>(prefixes: &[Prefix<T>], flags: &Flags) -> bool {
  if !flags.use_gcds {
    return false;
  }

  for p in prefixes {
    if p.gcd > T::Unsigned::ONE {
      return true;
    }
  }
  for (i, pi) in prefixes.iter().enumerate().skip(1) {
    let pj = &prefixes[i - 1];
    if pi.lower == pi.upper
      && pj.lower == pj.upper
      && pj.upper.to_unsigned() + T::Unsigned::ONE < pi.lower.to_unsigned()
    {
      return true;
    }
  }
  false
}

pub fn use_gcd_arithmetic<T: NumberLike>(prefixes: &[Prefix<T>]) -> bool {
  prefixes
    .iter()
    .any(|p| p.gcd > T::Unsigned::ONE && p.upper != p.lower)
}

pub fn gcd_bits_required<U: UnsignedLike>(range: U) -> usize {
  range.to_f64().log2().ceil() as usize
}

// to store gcd, we write and read gcd - 1 in the minimum number of bits
// since we know gcd <= upper - lower
pub fn write_gcd<U: UnsignedLike>(range: U, gcd: U, writer: &mut BitWriter) {
  let nontrivial = gcd != U::ONE;
  writer.write_one(nontrivial);
  if nontrivial {
    writer.write_diff(gcd - U::ONE, gcd_bits_required(range));
  }
}

pub fn read_gcd<U: UnsignedLike>(range: U, reader: &mut BitReader) -> QCompressResult<U> {
  if reader.read_one()? {
    let gcd_minus_one = reader.read_uint::<U>(gcd_bits_required(range))?;
    if gcd_minus_one >= range {
      Err(QCompressError::corruption(format!(
        "stored GCD was {} + 1, greater than range {}",
        gcd_minus_one, range,
      )))
    } else {
      Ok(gcd_minus_one + U::ONE)
    }
  } else {
    Ok(U::ONE)
  }
}

pub fn fold_prefix_gcds_left<U: UnsignedLike>(
  left_lower: U,
  left_upper: U,
  left_gcd: U,
  right_upper: U,
  acc: &mut Option<U>,
) {
  // folding GCD's involves GCD'ing with their modulo offset and (if the new
  // range is nontrivial) with the new prefix's GCD
  if left_upper != right_upper {
    *acc = Some(match *acc {
      Some(gcd) => pair_gcd(right_upper - left_upper, gcd),
      None => right_upper - left_upper,
    });
  }
  if left_upper != left_lower {
    *acc = Some(match *acc {
      Some(gcd) => pair_gcd(left_gcd, gcd),
      None => left_gcd,
    });
  }
}

pub trait GcdOperator<U: UnsignedLike> {
  fn get_offset(diff: U, gcd: U) -> U;
  fn get_diff(offset: U, gcd: U) -> U;
}

pub struct TrivialGcdOp;

pub struct GeneralGcdOp;

// When all prefix GCD's are 1
impl<U: UnsignedLike> GcdOperator<U> for TrivialGcdOp {
  fn get_offset(diff: U, _: U) -> U {
    diff
  }
  fn get_diff(offset: U, _: U) -> U {
    offset
  }
}

// General case when GCD might not be 1
impl<U: UnsignedLike> GcdOperator<U> for GeneralGcdOp {
  fn get_offset(diff: U, gcd: U) -> U {
    diff / gcd
  }
  fn get_diff(offset: U, gcd: U) -> U {
    offset * gcd
  }
}

#[cfg(test)]
mod tests {
  use crate::gcd_utils::*;

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
