use std::cmp::Ordering;
use std::convert::TryInto;

use crate::types::{NumberLike, SignedLike};
use crate::errors::QCompressResult;

impl SignedLike for bool {
  const ZERO: Self = false;

  fn wrapping_add(self, other: Self) -> Self {
    self ^ other
  }

  fn wrapping_sub(self, other: Self) -> Self {
    self ^ other
  }
}

impl NumberLike for bool {
  const HEADER_BYTE: u8 = 7;
  // it's easiest to use 8 bits per uncompressed boolean
  // because that's how rust represents them too
  const PHYSICAL_BITS: usize = 8;

  type Signed = bool;
  type Unsigned = u8;

  fn to_signed(self) -> bool {
    self
  }

  fn from_signed(signed: bool) -> bool {
    signed
  }

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

  fn to_bytes(self) -> Vec<u8> {
    vec![self as u8]
  }

  fn from_bytes(bytes: Vec<u8>) -> QCompressResult<bool> {
    Ok(u8::from_be_bytes(bytes.try_into().unwrap()) != 0)
  }
}
