use std::cmp::Ordering;
use std::convert::TryInto;

use crate::compressor::Compressor;
use crate::decompressor::Decompressor;
use crate::types::NumberLike;

impl NumberLike for i64 {
  const HEADER_BYTE: u8 = 1;
  const PHYSICAL_BITS: usize = 64;

  type Unsigned = u64;

  fn to_unsigned(self) -> u64 {
    self.wrapping_sub(i64::MIN) as u64
  }

  fn from_unsigned(off: u64) -> Self {
    i64::MIN.wrapping_add(off as i64)
  }

  fn num_eq(&self, other: &Self) -> bool {
    self.eq(other)
  }

  fn num_cmp(&self, other: &Self) -> Ordering {
    self.cmp(other)
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
