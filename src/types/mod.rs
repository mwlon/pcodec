use std::fmt::{Display, Debug};
use std::cmp::Ordering;
use std::cmp::Ordering::{Greater, Less};

pub mod float32;
pub mod float64;
pub mod signed32;
pub mod signed64;
pub mod unsigned32;
pub mod unsigned64;

pub trait NumberLike: Copy + Display + Debug {
  fn num_eq(&self, other: &Self) -> bool;

  fn num_cmp(&self, other: &Self) -> Ordering;

  fn le(&self, other: &Self) -> bool {
    match self.num_cmp(other) {
      Greater => false,
      _ => true,
    }
  }

  fn lt(&self, other: &Self) -> bool {
    match self.num_cmp(other) {
      Less => true,
      _ => false,
    }
  }

  fn ge(&self, other: &Self) -> bool {
    match self.num_cmp(other) {
      Less => false,
      _ => true,
    }
  }

  fn gt(&self, other: &Self) -> bool {
    match self.num_cmp(other) {
      Greater => true,
      _ => false,
    }
  }
}

pub trait DataType<T> where T: NumberLike {
  const HEADER_BYTE: u8;
  const BIT_SIZE: usize;
  const ZERO: T; // only shows up in unreachable code, so maybe we can remove it

  fn u64_diff(upper: T, lower: T) -> u64;
  fn add_u64(lower: T, off: u64) -> T;
  fn bytes_from(num: T) -> Vec<u8>;
  fn from_bytes(bytes: Vec<u8>) -> T;
}
