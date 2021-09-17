use std::cmp::Ordering;
use std::convert::TryInto;

use crate::compressor::Compressor;
use crate::decompressor::Decompressor;
use crate::types::NumberLike;

impl NumberLike for bool {
  const HEADER_BYTE: u8 = 7;
  const PHYSICAL_BITS: usize = 8;
  const LOGICAL_BITS: u32 = 1;

  type Diff = u8;

  fn num_eq(&self, other: &Self) -> bool {
    self.eq(other)
  }

  fn num_cmp(&self, other: &Self) -> Ordering {
    self.cmp(other)
  }

  fn offset_diff(upper: bool, lower: bool) -> u8 {
    (upper as u8) - (lower as u8)
  }

  fn add_offset(lower: bool, off: u8) -> bool {
    lower || (off > 0)
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
