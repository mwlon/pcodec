use std::cmp::Ordering;
use std::convert::TryInto;

use crate::compressor::Compressor;
use crate::decompressor::Decompressor;
use crate::types::NumberLike;

impl NumberLike for u32 {
  const HEADER_BYTE: u8 = 4;
  const PHYSICAL_BITS: usize = 32;
  const LOGICAL_BITS: u32 = 32;

  type Diff = u32;

  fn num_eq(&self, other: &Self) -> bool {
    self.eq(other)
  }

  fn num_cmp(&self, other: &Self) -> Ordering {
    self.cmp(other)
  }

  fn offset_diff(upper: u32, lower: u32) -> u32 {
    upper - lower
  }

  fn add_offset(lower: u32, off: u32) -> u32 {
    lower + off
  }

  fn bytes_from(num: u32) -> Vec<u8> {
    num.to_be_bytes().to_vec()
  }

  fn from_bytes(bytes: Vec<u8>) -> u32 {
    u32::from_be_bytes(bytes.try_into().unwrap())
  }
}

pub type U32Compressor = Compressor<u32>;
pub type U32Decompressor = Decompressor<u32>;
