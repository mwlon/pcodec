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

pub trait NumberLike: Copy + Debug + Display + Default {
  const HEADER_BYTE: u8;
  const BIT_SIZE: usize;

  fn num_eq(&self, other: &Self) -> bool;

  fn num_cmp(&self, other: &Self) -> Ordering;

  fn offset_diff(upper: Self, lower: Self) -> u64;

  fn add_offset(lower: Self, off: u64) -> Self;

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
