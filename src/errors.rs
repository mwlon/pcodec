use std::fmt::{Display, Formatter};
use std::fmt;
use crate::utils;
use crate::types::NumberLike;
use crate::utils::MAGIC_HEADER;
use std::error::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaxEntriesError {
  pub n: usize,
}

impl Display for MaxEntriesError {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "number of elements {} exceeded max number of elements {}",
      self.n,
      utils::MAX_ENTRIES,
    )
  }
}

impl Error for MaxEntriesError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaxDepthError {
  pub max_depth: u32,
}

impl Display for MaxDepthError {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "max depth {} exceeded max max depth of {}",
      self.max_depth,
      utils::MAX_MAX_DEPTH,
    )
  }
}

impl Error for MaxDepthError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutOfRangeError<T> where T: NumberLike {
  pub num: T,
}

impl<T> Display for OutOfRangeError<T> where T: NumberLike{
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "number {} was not found in any range",
      self.num,
    )
  }
}

impl<T> Error for OutOfRangeError<T> where T: NumberLike {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MisalignedBitReaderError {}

impl Display for MisalignedBitReaderError {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "cannot read_bytes on misaligned bit reader"
    )
  }
}

impl Error for MisalignedBitReaderError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MagicHeaderError {
  pub header: Vec<u8>,
}

impl Display for MagicHeaderError {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "header {:?} did not match qco expected header {:?}",
      self.header,
      MAGIC_HEADER,
    )
  }
}

impl Error for MagicHeaderError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeaderDtypeError {
  pub dtype_byte: u8,
  pub expected_byte: u8,
}

impl Error for HeaderDtypeError {}

impl Display for HeaderDtypeError {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "data type byte {} did not match expected data type byte {}",
      self.dtype_byte,
      self.expected_byte,
    )
  }
}

