use std::cmp::Ordering;
use std::convert::TryInto;

use crate::compressor::Compressor;
use crate::decompressor::Decompressor;
use crate::types::{DataType, NumberLike};

const SIGN_BIT_MASK: u32 = 1_u32 << 31;

impl NumberLike for f32 {
  fn num_eq(&self, other: &f32) -> bool {
    self.to_bits() == other.to_bits()
  }

  fn num_cmp(&self, other: &f32) -> Ordering {
    F32DataType::f32_to_u32(*self).cmp(&F32DataType::f32_to_u32(*other))
  }
}

pub struct F32DataType {}

impl F32DataType {
  fn f32_to_u32(x: f32) -> u32 {
    let mem_layout_x_u32 = x.to_bits();
    if mem_layout_x_u32 & SIGN_BIT_MASK > 0 {
      // negative float
      !mem_layout_x_u32
    } else {
      // positive float
      mem_layout_x_u32 ^ SIGN_BIT_MASK
    }
  }

  fn from_u32(x: u32) -> f32 {
    if x & SIGN_BIT_MASK > 0 {
      // positive float
      f32::from_bits(x ^ SIGN_BIT_MASK)
    } else {
      // negative float
      f32::from_bits(!x)
    }
  }
}

// Note that in all conversions between float and u64, we are using the u64 to indicate an offset.
// For instance, since f32 has 23 fraction bits, here we want 1.0 + 3_u64 to be
// 1.0 + (3.0 * 2.0 ^ -23).
impl DataType<f32> for F32DataType {
  const HEADER_BYTE: u8 = 6;
  const BIT_SIZE: usize = 32;

  fn offset_diff(upper: f32, lower: f32) -> u64 {
    (Self::f32_to_u32(upper) - Self::f32_to_u32(lower)) as u64
  }

  fn add_offset(lower: f32, off: u64) -> f32 {
    Self::from_u32(Self::f32_to_u32(lower) + off as u32)
  }

  fn bytes_from(num: f32) -> Vec<u8> {
    num.to_be_bytes().to_vec()
  }

  fn from_bytes(bytes: Vec<u8>) -> f32 {
    f32::from_be_bytes(bytes.try_into().unwrap())
  }
}

pub type F32Compressor = Compressor<i32, F32DataType>;
pub type F32Decompressor = Decompressor<i32, F32DataType>;
