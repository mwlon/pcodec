use std::cmp::Ordering;
use std::convert::TryInto;

use crate::compressor::Compressor;
use crate::decompressor::Decompressor;
use crate::types::NumberLike;

impl NumberLike for u64 {
  const HEADER_BYTE: u8 = 2;
  const PHYSICAL_BITS: usize = 64;

  type Unsigned = u64;

  fn to_unsigned(self) -> Self::Unsigned {
    self
  }

  fn from_unsigned(off: Self::Unsigned) -> Self {
    off
  }

  fn num_eq(&self, other: &Self) -> bool {
    self.eq(other)
  }

  fn num_cmp(&self, other: &Self) -> Ordering {
    self.cmp(other)
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
