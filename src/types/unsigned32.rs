use crate::types::{NumberLike, DataType};
use std::convert::TryInto;
use crate::compressor::Compressor;
use crate::decompressor::Decompressor;

pub struct U32DataType {}

impl NumberLike for u32 {}

impl DataType<u32> for U32DataType {
  const HEADER_BYTE: u8 = 4;
  const BIT_SIZE: usize = 32;
  const ZERO: u32 = 0;

  #[inline(always)]
  fn u64_diff(upper: u32, lower: u32) -> u64 {
    (upper - lower) as u64
  }

  #[inline(always)]
  fn add_u64(lower: u32, off: u64) -> u32 {
    lower + off as u32
  }

  fn bytes_from(num: u32) -> Vec<u8> {
    num.to_be_bytes().to_vec()
  }

  fn from_bytes(bytes: Vec<u8>) -> u32 {
    u32::from_be_bytes(bytes.try_into().unwrap())
  }
}

pub type U32Compressor = Compressor<u32, U32DataType>;
pub type U32Decompressor = Decompressor<u32, U32DataType>;
