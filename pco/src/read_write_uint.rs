use std::fmt::{Debug, Display};
use std::ops::{Add, BitAnd, BitAndAssign, BitOr, BitOrAssign, Shl, Shr, Sub};

use crate::constants::Bitlen;
use crate::data_types::UnsignedLike;

pub const fn calc_max_u64s(precision: Bitlen) -> usize {
  // See bit_reader::read_uint_at for an explanation of these thresholds.
  if precision == 0 {
    0
  } else if precision <= 57 {
    1
  } else if precision <= 113 {
    2
  } else {
    3
  }
}

pub trait ReadWriteUint:
  Add<Output = Self>
  + BitAnd<Output = Self>
  + BitOr<Output = Self>
  + BitAndAssign
  + BitOrAssign
  + Copy
  + Debug
  + Display
  + Shl<Bitlen, Output = Self>
  + Shr<Bitlen, Output = Self>
  + Sub<Output = Self>
{
  const ZERO: Self;
  const ONE: Self;
  const BITS: Bitlen;
  const MAX_U64S: usize = calc_max_u64s(Self::BITS);

  fn from_u64(x: u64) -> Self;
  fn to_u64(self) -> u64;
}

impl ReadWriteUint for usize {
  const ZERO: Self = 0;
  const ONE: Self = 1;
  const BITS: Bitlen = usize::BITS;

  #[inline]
  fn from_u64(x: u64) -> Self {
    x as Self
  }

  #[inline]
  fn to_u64(self) -> u64 {
    self as u64
  }
}

impl<U: UnsignedLike> ReadWriteUint for U {
  const ZERO: Self = <Self as UnsignedLike>::ZERO;
  const ONE: Self = <Self as UnsignedLike>::ONE;
  const BITS: Bitlen = <Self as UnsignedLike>::BITS;

  #[inline]
  fn from_u64(x: u64) -> Self {
    <Self as UnsignedLike>::from_u64(x)
  }

  #[inline]
  fn to_u64(self) -> u64 {
    <Self as UnsignedLike>::to_u64(self)
  }
}
