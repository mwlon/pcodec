use crate::constants::Bitlen;
use crate::data_types::UnsignedLike;

pub fn bits_to_usize(bits: &[bool]) -> usize {
  let mut res = 0;
  for (i, &b) in bits.iter().enumerate() {
    if b {
      res |= 1 << i;
    }
  }
  res
}

pub fn code_to_string(x: usize, n: Bitlen) -> String {
  let mut res = String::new();
  for i in 0..n {
    let char = if (x >> i) & 1 > 0 { '1' } else { '0' };
    res.push(char);
  }
  res
}

// This bumpy log gives a more accurate average number of offset bits used.
pub fn avg_offset_bits<U: UnsignedLike>(lower: U, upper: U, gcd: U) -> f64 {
  (U::BITS - ((upper - lower) / gcd).leading_zeros()) as f64
}

// The true Huffman cost of course depends on the tree. We can statistically
// model this cost and get slightly different bumpy log formulas,
// but I haven't found
// anything that beats a simple log. Plus it's computationally cheap.
pub fn avg_depth_bits(weight: usize, total_weight: usize) -> f64 {
  (total_weight as f64 / weight as f64).log2()
}

pub const fn ceil_div(x: usize, divisor: usize) -> usize {
  (x + divisor - 1) / divisor
}

pub fn words_to_bytes(words: &[usize]) -> Vec<u8> {
  // We can't just transmute because many machines are little-endian.
  words
    .iter()
    .flat_map(|w| w.to_le_bytes())
    .collect::<Vec<_>>()
}

pub fn bits_to_encode(max_number: usize) -> Bitlen {
  usize::BITS - max_number.leading_zeros()
}

pub fn bits_to_encode_offset<U: UnsignedLike>(max_offset: U) -> Bitlen {
  U::BITS - max_offset.leading_zeros()
}

pub const fn bits_to_encode_offset_bits<U: UnsignedLike>() -> Bitlen {
  (Bitlen::BITS - U::BITS.leading_zeros()) as Bitlen
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_code_to_string() {
    assert_eq!(code_to_string(0, 0), "".to_string());
    assert_eq!(code_to_string(1, 3), "100".to_string());
  }

  #[test]
  fn test_depth_bits() {
    assert_eq!(avg_depth_bits(2, 2), 0.0);
    assert_eq!(avg_depth_bits(2, 4), 1.0);
    assert_eq!(avg_depth_bits(2, 8), 2.0);
    assert_eq!(avg_depth_bits(4, 8), 1.0);
  }

  #[test]
  fn test_avg_offset_bits() {
    assert_eq!(avg_offset_bits(0_u32, 0_u32, 1), 0.0);
    assert_eq!(avg_offset_bits(4_u32, 5_u32, 1), 1.0);
    assert_eq!(avg_offset_bits(10_u32, 13_u32, 1), 2.0);
    assert_eq!(avg_offset_bits(10_u32, 13_u32, 3), 1.0);
    assert_eq!(avg_offset_bits(10_u32, 19_u32, 3), 2.0);
  }

  #[test]
  fn test_bits_to_encode_offset_bits() {
    assert_eq!(bits_to_encode_offset_bits::<u32>(), 6);
    assert_eq!(bits_to_encode_offset_bits::<u64>(), 7);
  }
}
