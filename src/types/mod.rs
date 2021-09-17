use std::cmp::Ordering;
use std::cmp::Ordering::{Greater, Less};
use std::fmt::{Debug, Display};
use std::ops::{BitOrAssign, BitAnd, Sub, Shl};

pub mod boolean;
pub mod float32;
pub mod float64;
pub mod signed32;
pub mod signed64;
pub mod timestamp_ns;
pub mod unsigned32;
pub mod unsigned64;

pub trait UnsignedLike: BitAnd<Output=Self> + BitOrAssign + Copy + Debug + Display + PartialOrd + Shl<u32, Output=Self> + Shl<usize, Output=Self> + Sub<Output=Self> + From<u8> {
  const ZERO: Self;
  const ONE: Self;
  const MAX: Self;
  const BITS: u32;

  fn from_f64(x: f64) -> Self;
  fn to_f64(self) -> f64;
}

macro_rules! impl_unsigned {
  ($t:ty) => {
    impl UnsignedLike for $t {
      const ZERO: Self = 0;
      const ONE: Self = 1;
      const MAX: Self = Self::MAX;
      const BITS: u32 = Self::BITS;

      fn from_f64(x: f64) -> Self {
        x as Self
      }

      fn to_f64(self) -> f64 {
        self as f64
      }
    }
  }
}

impl_unsigned!(u8);
impl_unsigned!(u32);
impl_unsigned!(u64);
impl_unsigned!(u128);

pub trait NumberLike: Copy + Debug + Display + Default {
  const HEADER_BYTE: u8;
  const PHYSICAL_BITS: usize;

  type Diff: UnsignedLike;

  fn num_eq(&self, other: &Self) -> bool;

  fn num_cmp(&self, other: &Self) -> Ordering;

  fn offset_diff(upper: Self, lower: Self) -> Self::Diff;

  fn add_offset(lower: Self, off: Self::Diff) -> Self;

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
