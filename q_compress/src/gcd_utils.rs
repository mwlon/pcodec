use crate::data_types::{NumberLike, UnsignedLike};
use crate::{BitReader, BitWriter, Prefix};
use crate::errors::{QCompressError, QCompressResult};
use crate::prefix::WeightedPrefix;

// fast if b is small, requires b > 0
pub fn pair_gcd<Diff: UnsignedLike>(mut a: Diff, mut b: Diff) -> Diff {
  loop {
    a %= b;
    if a == Diff::ZERO {
      return b;
    }
    b %= a;
    if b == Diff::ZERO {
      return a;
    }
  }
}

pub fn gcd<Diff: UnsignedLike>(sorted: &[Diff]) -> Diff {
  let lower = sorted[0];
  let upper = sorted[sorted.len() - 1];
  if lower == upper {
    return lower;
  }
  let mut res = upper - lower;
  for &x in sorted.iter().skip(1) {
    if res == Diff::ONE {
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
pub fn common_gcd_for_compress<T: NumberLike>(prefixes: &[Prefix<T>]) -> Option<T::Unsigned> {
  let mut nontrivial_ranges_share_gcd: bool = true;
  let mut gcd = None;
  for p in prefixes {
    if p.upper != p.lower {
      if gcd.is_none() {
        gcd = Some(p.gcd);
      } else {
        nontrivial_ranges_share_gcd = false;
      }
    }
  }

  match (prefixes.len(), nontrivial_ranges_share_gcd, gcd) {
    (0, _, _) => None,
    (_, false, _) => None,
    (_, true, Some(gcd)) => Some(gcd),
    (_, _, None) => Some(T::Unsigned::ONE),
  }
}

pub fn weighted_prefixes_have_common_gcd<T: NumberLike>(wps: &[WeightedPrefix<T>]) -> bool {
  let prefixes = wps.iter()
    .map(|wp| wp.prefix.clone())
    .collect::<Vec<_>>();
  common_gcd_for_compress(&prefixes).is_some()
}

pub fn gcd_bits_required<Diff: UnsignedLike>(range: Diff) -> usize {
  range.to_f64().log2().ceil() as usize
}

// to store gcd, we write and read gcd - 1 in the minimum number of bits
// since we know gcd <= upper - lower
pub fn write_gcd<Diff: UnsignedLike>(range: Diff, gcd: Diff, writer: &mut BitWriter) {
  let nontrivial = gcd != Diff::ONE;
  writer.write_one(nontrivial);
  if nontrivial {
    writer.write_diff(gcd - Diff::ONE, gcd_bits_required(range));
  }
}

pub fn read_gcd<Diff: UnsignedLike>(range: Diff, reader: &mut BitReader) -> QCompressResult<Diff> {
  if reader.read_one()? {
    let gcd_minus_one = reader.read_diff::<Diff>(gcd_bits_required(range))?;
    if gcd_minus_one >= range {
      Err(QCompressError::corruption(format!(
        "stored GCD was {} + 1, greater than range {}",
        gcd_minus_one,
        range,
      )))
    } else {
      Ok(gcd_minus_one + Diff::ONE)
    }
  } else {
    Ok(Diff::ONE)
  }
}

pub trait GcdOperator<Diff: UnsignedLike> {
  fn get_offset(diff: Diff, gcd: Diff) -> Diff;
  fn get_diff(offset: Diff, gcd: Diff) -> Diff;
}

pub struct TrivialGcdOp;

pub struct GeneralGcdOp;

// When all prefix GCD's are 1
impl<Diff: UnsignedLike> GcdOperator<Diff> for TrivialGcdOp {
  fn get_offset(diff: Diff, _: Diff) -> Diff {
    diff
  }
  fn get_diff(offset: Diff, _: Diff) -> Diff {
    offset
  }
}

// General case when GCD might not be 1
impl<Diff: UnsignedLike> GcdOperator<Diff> for GeneralGcdOp {
  fn get_offset(diff: Diff, gcd: Diff) -> Diff {
    diff / gcd
  }
  fn get_diff(offset: Diff, gcd: Diff) -> Diff {
    offset * gcd
  }
}

pub fn fold_prefix_gcds<Diff: UnsignedLike>(lower: Diff, upper: Diff, gcd: Diff, acc: &mut Diff) {
  // reducing GCD's involves taking their pairwise GCD, additionally
  // GCD'd with their modulo offset
  *acc = pair_gcd(
    upper - lower,
    pair_gcd(gcd, *acc)
  );
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
