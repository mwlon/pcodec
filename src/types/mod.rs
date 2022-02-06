use std::cmp::Ordering;
use std::cmp::Ordering::{Greater, Less};
use std::fmt::{Debug, Display};
use std::ops::{Add, BitAnd, BitOrAssign, Shl, Shr, Sub};

use crate::{BitReader, BitWriter};
use crate::bits;
use crate::errors::QCompressResult;

pub mod boolean;
pub mod floats;
pub mod signeds;
pub mod timestamps;
pub mod unsigneds;

pub trait SignedLike {
  const ZERO: Self;

  fn wrapping_add(self, other: Self) -> Self;
  fn wrapping_sub(self, other: Self) -> Self;
}

macro_rules! impl_signed {
  ($t: ty) => {
    impl SignedLike for $t {
      const ZERO: Self = 0;

      fn wrapping_add(self, other: Self) -> Self {
        self.wrapping_add(other)
      }

      fn wrapping_sub(self, other: Self) -> Self {
        self.wrapping_sub(other)
      }
    }
  }
}

impl_signed!(i8);
impl_signed!(i32);
impl_signed!(i64);
impl_signed!(i128);

pub trait UnsignedLike: Add<Output=Self> + BitAnd<Output=Self> + BitOrAssign +
Copy + Debug + Default + Display + From<u8> + PartialOrd +
Shl<u32, Output=Self> + Shl<usize, Output=Self> + Shr<usize, Output=Self> +
Sub<Output=Self> {
  const ZERO: Self;
  const ONE: Self;
  const MAX: Self;
  const BITS: usize;

  fn to_f64(self) -> f64;
  fn last_u8(self) -> u8;
}

macro_rules! impl_unsigned {
  ($t: ty) => {
    impl UnsignedLike for $t {
      const ZERO: Self = 0;
      const ONE: Self = 1;
      const MAX: Self = Self::MAX;
      const BITS: usize = Self::BITS as usize;

      fn to_f64(self) -> f64 {
        self as f64
      }

      fn last_u8(self) -> u8 {
        (self & 0xff) as u8
      }
    }
  }
}

impl_unsigned!(u8);
impl_unsigned!(u32);
impl_unsigned!(u64);
impl_unsigned!(u128);

pub trait NumberLike: Copy + Debug + Display + Default + PartialEq + 'static {
  const HEADER_BYTE: u8;
  const PHYSICAL_BITS: usize;

  type Unsigned: UnsignedLike;
  type Signed: SignedLike + NumberLike<Signed=Self::Signed, Unsigned=Self::Unsigned>;

  fn num_eq(&self, other: &Self) -> bool;

  fn num_cmp(&self, other: &Self) -> Ordering;

  fn to_unsigned(self) -> Self::Unsigned;

  fn from_unsigned(off: Self::Unsigned) -> Self;

  fn to_signed(self) -> Self::Signed;

  fn from_signed(signed: Self::Signed) -> Self;

  fn to_bytes(self) -> Vec<u8>;

  fn from_bytes(bytes: Vec<u8>) -> QCompressResult<Self>;

  fn read_from(reader: &mut BitReader) -> QCompressResult<Self> {
    let bools = reader.read(Self::PHYSICAL_BITS)?;
    Self::from_bytes(bits::bits_to_bytes(bools))
  }

  fn write_to(self, writer: &mut BitWriter) {
    writer.write_bytes(&self.to_bytes());
  }

  fn le(&self, other: &Self) -> bool {
    !matches!(self.num_cmp(other), Greater)
  }

  fn lt(&self, other: &Self) -> bool {
    matches!(self.num_cmp(other), Less)
  }

  fn ge(&self, other: &Self) -> bool {
    !matches!(self.num_cmp(other), Less)
  }

  fn gt(&self, other: &Self) -> bool {
    matches!(self.num_cmp(other), Greater)
  }
}
