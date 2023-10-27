use std::fmt::{Debug, Display};
use std::ops::{Add, BitAnd, BitAndAssign, BitOr, BitOrAssign, Shl, Shr, Sub};

use crate::constants::{Bitlen, WORD_BITLEN};
use crate::data_types::UnsignedLike;

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
  const MAX_EXTRA_WORDS: Bitlen = (Self::BITS + 6) / WORD_BITLEN;

  fn from_word(word: usize) -> Self;
  fn to_usize(self) -> usize;
}

impl ReadWriteUint for usize {
  const ZERO: Self = 0;
  const ONE: Self = 1;
  const BITS: Bitlen = WORD_BITLEN;

  #[inline]
  fn from_word(word: usize) -> Self {
    word
  }

  #[inline]
  fn to_usize(self) -> usize {
    self
  }
}

impl<U: UnsignedLike> ReadWriteUint for U {
  const ZERO: Self = <Self as UnsignedLike>::ZERO;
  const ONE: Self = <Self as UnsignedLike>::ONE;
  const BITS: Bitlen = <Self as UnsignedLike>::BITS;

  #[inline]
  fn from_word(word: usize) -> Self {
    <Self as UnsignedLike>::from_word(word)
  }

  #[inline]
  fn to_usize(self) -> usize {
    <Self as UnsignedLike>::to_usize(self)
  }
}
