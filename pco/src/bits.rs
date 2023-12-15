use crate::constants::Bitlen;
use crate::data_types::UnsignedLike;
use crate::read_write_uint::ReadWriteUint;

// doesn't handle the case when n >= U::BITS
#[inline]
pub fn lowest_bits_fast<U: ReadWriteUint>(x: U, n: Bitlen) -> U {
  x & ((U::ONE << n) - U::ONE)
}

#[inline]
pub fn lowest_bits<U: ReadWriteUint>(x: U, n: Bitlen) -> U {
  if n >= U::BITS {
    x
  } else {
    lowest_bits_fast(x, n)
  }
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
  fn test_lowest_bits() {
    assert_eq!(lowest_bits(u32::MAX, 0), 0);
    assert_eq!(lowest_bits_fast(u32::MAX, 0), 0);
    assert_eq!(lowest_bits(u32::MAX, 3), 7);
    assert_eq!(lowest_bits_fast(u32::MAX, 3), 7);
    assert_eq!(lowest_bits(u32::MAX, 32), u32::MAX);
  }

  #[test]
  fn test_bits_to_encode_offset_bits() {
    assert_eq!(bits_to_encode_offset_bits::<u32>(), 6);
    assert_eq!(bits_to_encode_offset_bits::<u64>(), 7);
  }
}
