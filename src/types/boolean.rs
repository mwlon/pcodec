use std::cmp::Ordering;
use std::convert::TryInto;

use crate::compressor::Compressor;
use crate::decompressor::Decompressor;
use crate::types::{DataType, NumberLike};

impl NumberLike for bool {
  fn num_eq(&self, other: &Self) -> bool {    
    self.eq(other)
  }

  fn num_cmp(&self, other: &Self) -> Ordering {
    self.cmp(other)
  }
}

pub struct BoolDataType {}

impl DataType<bool> for BoolDataType {
  const HEADER_BYTE: u8 = 7;
  const BIT_SIZE: usize = 8;

  fn offset_diff(upper: bool, lower: bool) -> u64 {
    (upper as u64) - (lower as u64)
  }

  fn add_offset(lower: bool, off: u64) -> bool {
    (lower as u64 + off) != 0
  }

  fn bytes_from(value: bool) -> Vec<u8> {
    vec![value as u8]
  }

  fn from_bytes(bytes: Vec<u8>) -> bool {
    bytes[0] != 0
  }
}

pub type BoolCompressor = Compressor<u8, BoolDataType>;
pub type BoolDecompressor = Decompressor<u8, BoolDataType>;
