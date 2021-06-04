use std::fmt::Display;

pub mod signed32;
pub mod signed64;
pub mod unsigned32;
pub mod unsigned64;

pub trait NumberLike: Copy + Ord + Eq + Display {}

pub trait DataType<T> where T: NumberLike {
  const HEADER_BYTE: u8;
  const BIT_SIZE: usize;
  const ZERO: T; // only shows up in unreachable code, so maybe we can remove it
  fn u64_diff(upper: T, lower: T) -> u64;
  fn add_u64(lower: T, off: u64) -> T;
  fn bytes_from(num: T) -> Vec<u8>;
  fn from_bytes(bytes: Vec<u8>) -> T;
}

