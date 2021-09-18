use std::cmp::Ordering;
use std::convert::TryInto;

use crate::compressor::Compressor;
use crate::decompressor::Decompressor;
use crate::types::NumberLike;

impl NumberLike for i64 {
  const HEADER_BYTE: u8 = 1;
  const PHYSICAL_BITS: usize = 64;

  type Diff = u64;

  fn num_eq(&self, other: &Self) -> bool {
    self.eq(other)
  }

  fn num_cmp(&self, other: &Self) -> Ordering {
    self.cmp(other)
  }

  fn offset_diff(upper: i64, lower: i64) -> u64 {
    upper.wrapping_sub(lower) as u64
  }

  fn add_offset(lower: i64, off: u64) -> i64 {
    lower.wrapping_add(off as i64)
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
