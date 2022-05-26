use crate::data_types::{NumberLike, UnsignedLike};
use crate::{BitReader, BitWriter, Prefix};
use crate::errors::{QCompressError, QCompressResult};

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
    return Diff::ONE;
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

pub fn get_common_gcd<T: NumberLike>(prefixes: &[Prefix<T>]) -> Option<T::Unsigned> {
  if prefixes.is_empty() {
    None
  } else {
    let gcd = prefixes[0].gcd;
    for p in prefixes.iter().skip(1) {
      if p.gcd != gcd {
        return None;
      }
    }
    Some(gcd)
  }
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
