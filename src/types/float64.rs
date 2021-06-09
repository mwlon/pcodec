use std::cmp::Ordering;
use std::convert::TryInto;

use crate::compressor::Compressor;
use crate::decompressor::Decompressor;
use crate::types::{DataType, NumberLike};

const SIGN_BIT_MASK: u64 = 1_u64 << 63;

impl NumberLike for f64 {
  #[inline(always)]
  fn num_eq(&self, other: &f64) -> bool {
    F64DataType::f64_to_u64(*self) == F64DataType::f64_to_u64(*other)
  }

  #[inline(always)]
  fn num_cmp(&self, other: &f64) -> Ordering {
    F64DataType::f64_to_u64(*self).num_cmp(&F64DataType::f64_to_u64(*other))
  }
}

pub struct F64DataType {}

impl F64DataType {
  #[inline(always)]
  fn f64_to_u64(x: f64) -> u64 {
    let mem_layout_x_u64 = x.to_bits();
    if mem_layout_x_u64 & SIGN_BIT_MASK > 0 {
      // negative float
      !mem_layout_x_u64
    } else {
      // positive float
      mem_layout_x_u64 ^ SIGN_BIT_MASK
    }
  }

  #[inline(always)]
  fn from_u64(x: u64) -> f64 {
    if x & SIGN_BIT_MASK > 0 {
      // positive float
      f64::from_bits(x ^ SIGN_BIT_MASK)
    } else {
      // negative float
      f64::from_bits(!x)
    }
  }
}

// Note that in all conversions between float and u64, we are using the u64 to indicate an offset.
// For instance, since f64 has 52 fraction bits, here we want 1.0 + 3_u64 to be
// 1.0 + (3.0 * 2.0 ^ -52).
impl DataType<f64> for F64DataType {
  const HEADER_BYTE: u8 = 5;
  const BIT_SIZE: usize = 64;
  const ZERO: f64 = 0.0;

  #[inline(always)]
  fn u64_diff(upper: f64, lower: f64) -> u64 {
    Self::f64_to_u64(upper) - Self::f64_to_u64(lower)
  }

  #[inline(always)]
  fn add_u64(lower: f64, off: u64) -> f64 {
    Self::from_u64(Self::f64_to_u64(lower) + off)
  }

  fn bytes_from(num: f64) -> Vec<u8> {
    num.to_be_bytes().to_vec()
  }

  fn from_bytes(bytes: Vec<u8>) -> f64 {
    f64::from_be_bytes(bytes.try_into().unwrap())
  }
}

pub type F64Compressor = Compressor<i64, F64DataType>;
pub type F64Decompressor = Decompressor<i64, F64DataType>;
