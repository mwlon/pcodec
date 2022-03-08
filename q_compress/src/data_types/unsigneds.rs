use std::convert::TryInto;

use crate::data_types::{NumberLike, UnsignedLike};
use crate::errors::QCompressResult;

macro_rules! impl_unsigned {
  ($t: ty) => {
    impl UnsignedLike for $t {
      const ZERO: Self = 0;
      const ONE: Self = 1;
      const MAX: Self = Self::MAX;
      const BITS: usize = Self::BITS as usize;

      fn from_word(word: usize) -> Self {
        word as Self
      }

      fn to_f64(self) -> f64 {
        self as f64
      }

      // These lshift an rshift implementations look slow, but all the
      // conditionals and masks are zero-costed away by the compiler.
      fn rshift_word(self, shift: usize) -> usize {
        if Self::BITS <= usize::BITS {
          (self as usize) >> shift
        } else {
          ((self >> shift) & (usize::MAX as Self)) as usize
        }
      }

      fn lshift_word(self, shift: usize) -> usize {
        if Self::BITS <= usize::BITS {
          (self as usize) << shift
        } else {
          ((self << shift) & (usize::MAX as Self)) as usize
        }
      }
    }
  }
}

impl_unsigned!(u8);
impl_unsigned!(u16);
impl_unsigned!(u32);
impl_unsigned!(u64);
impl_unsigned!(u128);

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

      fn to_bytes(self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
      }

      fn from_bytes(bytes: Vec<u8>) -> QCompressResult<Self> {
        Ok(Self::from_be_bytes(bytes.try_into().unwrap()))
      }
    }
  }
}

impl_unsigned_number!(u16, i16, 12);
impl_unsigned_number!(u32, i32, 4);
impl_unsigned_number!(u64, i64, 2);
impl_unsigned_number!(u128, i128, 11);
