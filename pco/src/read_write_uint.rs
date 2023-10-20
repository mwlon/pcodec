use std::ops::{Add, BitAnd, BitAndAssign, BitOr, BitOrAssign, Shl, Shr, Sub};
use std::fmt::{Debug, Display};
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
  const MAX: Self;
  const BITS: Bitlen;
  const MAX_EXTRA_WORDS: Bitlen = (Self::BITS + 6) / WORD_BITLEN;

  fn from_word(word: usize) -> Self;
}

impl ReadWriteUint for usize {
  const ZERO: Self = 0;
  const ONE: Self = 1;
  const MAX: Self = usize::MAX;
  const BITS: Bitlen = WORD_BITLEN;

  #[inline]
  fn from_word(word: usize) -> Self {
    word
  }
}

impl<U: UnsignedLike> ReadWriteUint for U {
  const ZERO: Self = <Self as UnsignedLike>::ZERO;
  const ONE: Self = <Self as UnsignedLike>::ONE;
  const MAX: Self = <Self as UnsignedLike>::MAX;
  const BITS: Bitlen = <Self as UnsignedLike>::BITS;

  #[inline]
  fn from_word(word: usize) -> Self {
    <Self as UnsignedLike>::from_word(word)
  }
}
