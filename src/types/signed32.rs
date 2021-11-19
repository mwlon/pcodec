use std::cmp::Ordering;
use std::convert::TryInto;

use crate::compressor::Compressor;
use crate::decompressor::Decompressor;
use crate::types::NumberLike;

impl NumberLike for i32 {
  const HEADER_BYTE: u8 = 3;
  const PHYSICAL_BITS: usize = 32;

  type Unsigned = u32;

  fn to_unsigned(self) -> u32 {
    self.wrapping_sub(i32::MIN) as u32
  }

  fn from_unsigned(off: u32) -> Self {
    i32::MIN.wrapping_add(off as i32)
  }

  fn num_eq(&self, other: &Self) -> bool {
    self.eq(other)
  }

  fn num_cmp(&self, other: &Self) -> Ordering {
    self.cmp(other)
  }

  fn bytes_from(num: i32) -> Vec<u8> {
    num.to_be_bytes().to_vec()
  }

  fn from_bytes(bytes: Vec<u8>) -> i32 {
    i32::from_be_bytes(bytes.try_into().unwrap())
  }
}

pub type I32Compressor = Compressor<i32>;
pub type I32Decompressor = Decompressor<i32>;
