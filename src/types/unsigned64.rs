use std::cmp::Ordering;
use std::convert::TryInto;

use crate::compressor::Compressor;
use crate::decompressor::Decompressor;
use crate::types::{DataType, NumberLike};

impl NumberLike for u64 {
  #[inline(always)]
  fn num_eq(&self, other: &Self) -> bool {
    self.eq(other)
  }

  #[inline(always)]
  fn num_cmp(&self, other: &Self) -> Ordering {
    self.cmp(other)
  }
}

pub struct U64DataType {}

impl DataType<u64> for U64DataType {
  const HEADER_BYTE: u8 = 2;
  const BIT_SIZE: usize = 64;
  const ZERO: u64 = 0;

  #[inline(always)]
  fn offset_diff(upper: u64, lower: u64) -> u64 {
    upper - lower
  }

  #[inline(always)]
  fn add_offset(lower: u64, off: u64) -> u64 {
    lower + off
  }

  fn bytes_from(num: u64) -> Vec<u8> {
    num.to_be_bytes().to_vec()
  }

  fn from_bytes(bytes: Vec<u8>) -> u64 {
    u64::from_be_bytes(bytes.try_into().unwrap())
  }
}

pub type U64Compressor = Compressor<u64, U64DataType>;
pub type U64Decompressor = Decompressor<u64, U64DataType>;
