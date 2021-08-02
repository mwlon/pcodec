use std::cmp::Ordering;
use std::convert::TryInto;

use crate::compressor::Compressor;
use crate::decompressor::Decompressor;
use crate::types::{DataType, NumberLike};

impl NumberLike for i64 {
  fn num_eq(&self, other: &Self) -> bool {
    self.eq(other)
  }

  fn num_cmp(&self, other: &Self) -> Ordering {
    self.cmp(other)
  }

  type DT = I64DataType;
}

pub struct I64DataType {}

impl DataType<i64> for I64DataType {
  const HEADER_BYTE: u8 = 1;
  const BIT_SIZE: usize = 64;

  fn offset_diff(upper: i64, lower: i64) -> u64 {
    if lower >= 0 {
      (upper - lower) as u64
    } else if lower == upper {
      0
    } else {
      let pos_lower_p1 = (lower + 1).abs() as u64;
      if upper >= 0 {
        (upper as u64) + pos_lower_p1 + 1
      } else {
        (pos_lower_p1 + 1) - (upper.abs() as u64)
      }
    }
  }

  fn add_offset(lower: i64, off: u64) -> i64 {
    if lower >= 0 {
      (lower as u64 + off) as i64
    } else if off == 0 {
      lower
    } else {
      let negi = (-lower) as u64;
      if negi <= off {
        (off - negi) as i64
      } else {
        -((negi - off) as i64)
      }
    }
  }

  fn bytes_from(num: i64) -> Vec<u8> {
    num.to_be_bytes().to_vec()
  }

  fn from_bytes(bytes: Vec<u8>) -> i64 {
    i64::from_be_bytes(bytes.try_into().unwrap())
  }
}

pub type I64Compressor = Compressor<i64>;
pub type I64Decompressor = Decompressor<i64>;
