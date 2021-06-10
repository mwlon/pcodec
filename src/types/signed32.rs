use std::cmp::Ordering;
use std::convert::TryInto;

use crate::compressor::Compressor;
use crate::decompressor::Decompressor;
use crate::types::{DataType, NumberLike};

impl NumberLike for i32 {
  #[inline(always)]
  fn num_eq(&self, other: &Self) -> bool {
    self.eq(other)
  }

  #[inline(always)]
  fn num_cmp(&self, other: &Self) -> Ordering {
    self.cmp(other)
  }
}

pub struct I32DataType {}

impl DataType<i32> for I32DataType {
  const HEADER_BYTE: u8 = 3;
  const BIT_SIZE: usize = 32;
  const ZERO: i32 = 0;

  #[inline(always)]
  fn offset_diff(upper: i32, lower: i32) -> u64 {
    (upper as i64 - lower as i64) as u64
  }

  #[inline(always)]
  fn add_offset(lower: i32, off: u64) -> i32 {
    if lower >= 0 {
      (lower as u64 + off) as i32
    } else if off == 0 {
      lower
    } else {
      let negi = (-lower) as u64;
      if negi <= off {
        (off - negi) as i32
      } else {
        -((negi - off) as i32)
      }
    }
  }

  fn bytes_from(num: i32) -> Vec<u8> {
    num.to_be_bytes().to_vec()
  }

  fn from_bytes(bytes: Vec<u8>) -> i32 {
    i32::from_be_bytes(bytes.try_into().unwrap())
  }
}

pub type I32Compressor = Compressor<i32, I32DataType>;
pub type I32Decompressor = Decompressor<i32, I32DataType>;
