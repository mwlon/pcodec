use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fmt;

use crate::constants::{MAGIC_HEADER, MAX_ENTRIES, MAX_MAX_DEPTH};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QCompressError where {
  MaxEntriesError { n: usize },
  MaxDepthError { max_depth: u32},
  OutOfRangeError { num_string: String },
  MisalignedError {},
  MagicHeaderError { header: Vec<u8> },
  HeaderDtypeError { header_byte: u8, decompressor_byte: u8 },
  InvalidTimestampError { parts: i128, parts_per_sec: u32 },
}

impl Display for QCompressError {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    match self {
      QCompressError::MaxEntriesError {n} => write!(
        f,
        "number of elements {} exceeded max number of elements {}",
        n,
        MAX_ENTRIES,
      ),
      QCompressError::MaxDepthError {max_depth} => write!(
        f,
        "max depth {} exceeded max max depth of {}",
        max_depth,
        MAX_MAX_DEPTH,
      ),
      QCompressError::OutOfRangeError {num_string} => write!(
        f,
        "number {} was not found in any range",
        num_string,
      ),
      QCompressError::MisalignedError {} => write!(
        f,
        "cannot read_bytes on misaligned bit reader"
      ),
      QCompressError::MagicHeaderError { header } => write!(
        f,
        "header {:?} did not match qco expected header {:?}",
        header,
        MAGIC_HEADER,
      ),
      QCompressError::HeaderDtypeError {header_byte, decompressor_byte} => write!(
        f,
        "header byte {} did not match expected decompressor data type byte {}",
        header_byte,
        decompressor_byte,
      ),
      QCompressError::InvalidTimestampError { parts, parts_per_sec } => write!(
        f,
        "invalid timestamp with {}/{} nanos",
        parts,
        parts_per_sec
      )
    }
  }
}

impl Error for QCompressError {}
