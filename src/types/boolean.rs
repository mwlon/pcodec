use std::cmp::Ordering;
use std::convert::TryInto;

use crate::compressor::Compressor;
use crate::decompressor::Decompressor;
use crate::types::NumberLike;

impl NumberLike for bool {
  const HEADER_BYTE: u8 = 7;
  // it's easiest to use 8 bits per uncompressed boolean
  // because that's how rust represents them too
  const PHYSICAL_BITS: usize = 8;

  type Unsigned = u8;

  fn to_unsigned(self) -> u8 {
    self as u8
  }

  fn from_unsigned(off: u8) -> bool {
    off > 0
  }

  fn num_eq(&self, other: &Self) -> bool {
    self.eq(other)
  }

  fn num_cmp(&self, other: &Self) -> Ordering {
    self.cmp(other)
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
