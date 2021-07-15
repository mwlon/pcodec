use std::fmt::{Display, Debug};
use std::cmp::Ordering;
use std::cmp::Ordering::{Greater, Less};

pub mod float32;
pub mod float64;
pub mod signed32;
pub mod signed64;
pub mod unsigned32;
pub mod unsigned64;
pub mod boolean;

pub trait NumberLike: Copy + Display + Debug + Default {
  fn num_eq(&self, other: &Self) -> bool;

  fn num_cmp(&self, other: &Self) -> Ordering;

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

pub trait DataType<T> where T: NumberLike {
  const HEADER_BYTE: u8;
  const BIT_SIZE: usize;

  fn offset_diff(upper: T, lower: T) -> u64;
  fn add_offset(lower: T, off: u64) -> T;
  fn bytes_from(num: T) -> Vec<u8>;
  fn from_bytes(bytes: Vec<u8>) -> T;
}
