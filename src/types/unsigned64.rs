use std::cmp::Ordering;
use std::convert::TryInto;

use crate::compressor::Compressor;
use crate::decompressor::Decompressor;
use crate::types::NumberLike;

impl NumberLike for u64 {
  const HEADER_BYTE: u8 = 2;
  const BIT_SIZE: usize = 64;

  fn num_eq(&self, other: &Self) -> bool {
    self.eq(other)
  }

  fn num_cmp(&self, other: &Self) -> Ordering {
    self.cmp(other)
  }

  fn offset_diff(upper: u64, lower: u64) -> u64 {
    upper - lower
  }

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

pub type U64Compressor = Compressor<u64>;
pub type U64Decompressor = Decompressor<u64>;
