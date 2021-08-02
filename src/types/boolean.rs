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

  type DT = BoolDataType;
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
    (value as u8).to_be_bytes().to_vec()
  }

  fn from_bytes(bytes: Vec<u8>) -> bool {
    u8::from_be_bytes(bytes.try_into().unwrap()) != 0
  }
}

pub type BoolCompressor = Compressor<u8>;
pub type BoolDecompressor = Decompressor<u8>;
