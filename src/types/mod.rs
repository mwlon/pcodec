use std::cmp::Ordering;
use std::cmp::Ordering::{Greater, Less};
use std::fmt::{Debug, Display};
use std::ops::{Add, BitAnd, BitOrAssign, Shl, Shr, Sub};

pub mod boolean;
pub mod float32;
pub mod float64;
pub mod signed32;
pub mod signed64;
pub mod timestamps;
pub mod unsigned32;
pub mod unsigned64;

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
  ($t:ty) => {
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

  fn num_eq(&self, other: &Self) -> bool;

  fn num_cmp(&self, other: &Self) -> Ordering;

  fn to_unsigned(self) -> Self::Unsigned;

  fn from_unsigned(off: Self::Unsigned) -> Self;

  fn bytes_from(num: Self) -> Vec<u8>;

  fn from_bytes(bytes: Vec<u8>) -> Self;

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
