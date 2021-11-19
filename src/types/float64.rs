use std::cmp::Ordering;
use std::convert::TryInto;

use crate::compressor::Compressor;
use crate::decompressor::Decompressor;
use crate::types::NumberLike;

const SIGN_BIT_MASK: u64 = 1_u64 << 63;

// Note that in all conversions between float and u64, we are using the u64 to indicate an offset.
// For instance, since f64 has 52 fraction bits, here we want 1.0 + 3_u64 to be
// 1.0 + (3.0 * 2.0 ^ -52).
impl NumberLike for f64 {
  const HEADER_BYTE: u8 = 5;
  const PHYSICAL_BITS: usize = 64;

  type Unsigned = u64;

  fn to_unsigned(self) -> u64 {
    let mem_layout_u64 = self.to_bits();
    if mem_layout_u64 & SIGN_BIT_MASK > 0 {
      // negative float
      !mem_layout_u64
    } else {
      // positive float
      mem_layout_u64 ^ SIGN_BIT_MASK
    }
  }

  fn from_unsigned(off: u64) -> Self {
    if off & SIGN_BIT_MASK > 0 {
      // positive float
      f64::from_bits(off ^ SIGN_BIT_MASK)
    } else {
      // negative float
      f64::from_bits(!off)
    }
  }

  fn num_eq(&self, other: &f64) -> bool {
    self.to_bits() == other.to_bits()
  }

  fn num_cmp(&self, other: &f64) -> Ordering {
    self.to_unsigned().cmp(&other.to_unsigned())
  }

  fn bytes_from(num: f64) -> Vec<u8> {
    num.to_be_bytes().to_vec()
  }

  fn from_bytes(bytes: Vec<u8>) -> f64 {
    f64::from_be_bytes(bytes.try_into().unwrap())
  }
}

pub type F64Compressor = Compressor<f64>;
pub type F64Decompressor = Decompressor<f64>;
