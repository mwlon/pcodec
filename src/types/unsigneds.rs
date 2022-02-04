use std::cmp::Ordering;
use std::convert::TryInto;

use crate::types::NumberLike;
use crate::compressor::Compressor;
use crate::decompressor::Decompressor;

macro_rules! impl_unsigned_number {
  ($t: ty, $signed: ty, $header_byte: expr) => {
    impl NumberLike for $t {
      const HEADER_BYTE: u8 = $header_byte;
      const PHYSICAL_BITS: usize = Self::BITS as usize;

      type Signed = $signed;
      type Unsigned = Self;

      fn to_signed(self) -> Self::Signed {
        (self as $signed).wrapping_add(<$signed>::MIN)
      }

      fn from_signed(signed: Self::Signed) -> Self {
        signed.wrapping_sub(<$signed>::MIN) as Self
      }

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

      fn to_bytes(self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
      }

      fn from_bytes(bytes: Vec<u8>) -> Self {
        Self::from_be_bytes(bytes.try_into().unwrap())
      }
    }
  }
}

impl_unsigned_number!(u32, i32, 4);
impl_unsigned_number!(u64, i64, 2);

pub type U32Compressor = Compressor<u32>;
pub type U32Decompressor = Decompressor<u32>;

pub type U64Compressor = Compressor<u64>;
pub type U64Decompressor = Decompressor<u64>;
