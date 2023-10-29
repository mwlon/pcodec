use crate::constants::{Bitlen, Weight};
use crate::data_types::UnsignedLike;
use crate::read_write_uint::ReadWriteUint;

#[inline]
pub fn lowest_bits<U: ReadWriteUint>(x: U, n: Bitlen) -> U {
  if n >= U::BITS {
    x
  } else {
    x & ((U::ONE << n) - U::ONE)
  }
}

// The true Huffman cost of course depends on the tree. We can statistically
// model this cost and get slightly different bumpy log formulas,
// but I haven't found
// anything that beats a simple log. Plus it's computationally cheap.
pub fn avg_depth_bits(weight: Weight, total_weight: usize) -> f64 {
  (total_weight as f64 / weight as f64).log2()
}

// TODO upgrade to rust 1.73 and delete this
pub const fn ceil_div(x: usize, divisor: usize) -> usize {
  (x + divisor - 1) / divisor
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
  fn test_depth_bits() {
    assert_eq!(avg_depth_bits(2, 2), 0.0);
    assert_eq!(avg_depth_bits(2, 4), 1.0);
    assert_eq!(avg_depth_bits(2, 8), 2.0);
    assert_eq!(avg_depth_bits(4, 8), 1.0);
  }

  #[test]
  fn test_bits_to_encode_offset_bits() {
    assert_eq!(bits_to_encode_offset_bits::<u32>(), 6);
    assert_eq!(bits_to_encode_offset_bits::<u64>(), 7);
  }
}
