use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fmt;

use crate::constants::*;
use crate::bits;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QCompressError where {
  CompatibilityError,
  CompressedBodySize { expected: usize, actual: usize },
  HeaderDtypeError { header_byte: u8, decompressor_byte: u8 },
  HuffmanTreeError { bits: Vec<bool>, issue: &'static str },
  InvalidTimestampError { parts: i128, parts_per_sec: u32 },
  MagicChunkByteError { byte: u8 },
  MagicHeaderError { header: Vec<u8> },
  MaxDepthError { max_depth: u32},
  MaxEntriesError { n: usize },
  MisalignedError,
  OutOfRangeError { num_string: String },
}

impl Display for QCompressError {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    match self {
      QCompressError::CompatibilityError => write!(
        f,
        "file contains newer flags than this version of q_compress supports",
      ),
      QCompressError::CompressedBodySize {expected, actual} => write!(
        f,
        "expected compressed body size of {} but consumed {} to read nums",
        expected,
        actual,
      ),
      QCompressError::HeaderDtypeError {header_byte, decompressor_byte} => write!(
        f,
        "header byte {} did not match expected decompressor data type byte {}",
        header_byte,
        decompressor_byte,
      ),
      QCompressError::HuffmanTreeError {bits, issue} => write!(
        f,
        "corrupt chunk metadata; prefix {} is {}",
        bits::bits_to_string(bits),
        issue,
      ),
      QCompressError::InvalidTimestampError { parts, parts_per_sec } => write!(
        f,
        "invalid timestamp with {}/{} nanos",
        parts,
        parts_per_sec
      ),
      QCompressError::MagicChunkByteError { byte } => write!(
        f,
        "expected either magic chunk byte {} or magic termination byte {} but got {}",
        MAGIC_CHUNK_BYTE,
        MAGIC_TERMINATION_BYTE,
        byte,
      ),
      QCompressError::MagicHeaderError { header } => write!(
        f,
        "header {:?} did not match qco expected header {:?}",
        header,
        MAGIC_HEADER,
      ),
      QCompressError::MaxDepthError {max_depth} => write!(
        f,
        "max depth {} exceeded max max depth of {}",
        max_depth,
        MAX_MAX_DEPTH,
      ),
      QCompressError::MaxEntriesError {n} => write!(
        f,
        "number of elements {} exceeded max number of elements {}",
        n,
        MAX_ENTRIES,
      ),
      QCompressError::MisalignedError => write!(
        f,
        "cannot perform byte-wise operation on misaligned bit reader or writer"
      ),
      QCompressError::OutOfRangeError {num_string} => write!(
        f,
        "number {} was not found in any range",
        num_string,
      ),
    }
  }
}

impl Error for QCompressError {}

pub type QCompressResult<T> = Result<T, QCompressError>;
