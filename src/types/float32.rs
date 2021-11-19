use std::cmp::Ordering;
use std::convert::TryInto;

use crate::compressor::Compressor;
use crate::decompressor::Decompressor;
use crate::types::NumberLike;

const SIGN_BIT_MASK: u32 = 1_u32 << 31;

// Note that in all conversions between float and u32, we are using the u32 to indicate an offset.
// For instance, since f32 has 23 fraction bits, here we want 1.0 + 3_u32 to be
// 1.0 + (3.0 * 2.0 ^ -23).
impl NumberLike for f32 {
  const HEADER_BYTE: u8 = 6;
  const PHYSICAL_BITS: usize = 32;

  type Unsigned = u32;

  fn to_unsigned(self) -> u32 {
    let mem_layout_u32 = self.to_bits();
    if mem_layout_u32 & SIGN_BIT_MASK > 0 {
      // negative float
      !mem_layout_u32
    } else {
      // positive float
      mem_layout_u32 ^ SIGN_BIT_MASK
    }
  }

  fn from_unsigned(off: u32) -> Self {
    if off & SIGN_BIT_MASK > 0 {
      // positive float
      f32::from_bits(off ^ SIGN_BIT_MASK)
    } else {
      // negative float
      f32::from_bits(!off)
    }
  }

  fn num_eq(&self, other: &f32) -> bool {
    self.to_bits() == other.to_bits()
  }

  fn num_cmp(&self, other: &f32) -> Ordering {
    self.to_unsigned().cmp(&other.to_unsigned())
  }

  fn bytes_from(num: f32) -> Vec<u8> {
    num.to_be_bytes().to_vec()
  }

  fn from_bytes(bytes: Vec<u8>) -> f32 {
    f32::from_be_bytes(bytes.try_into().unwrap())
  }
}

pub type F32Compressor = Compressor<f32>;
pub type F32Decompressor = Decompressor<f32>;
