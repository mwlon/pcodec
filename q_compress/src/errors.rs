use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fmt;

/// The different kinds of errors for `q_compress`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
  /// `Compatibility` errors occur during decompression, indicating the library
  /// version is not up-to-date enough for the provided data.
  Compatibility,
  /// `Corruption` errors occur during decompression, indicating the
  /// provided data is inconsistent or violates the Quantile Compression format.
  Corruption,
  /// `InsufficientData` errors occur during decompression, indicating
  /// the decompressor reached the end of the provided data before finishing.
  InsufficientData,
  /// `InvalidArgument` errors usually occur during compression, indicating
  /// the parameters provided to a function were invalid.
  InvalidArgument,
}

/// The error type used in results for all `q_compress` functionality.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QCompressError {
  pub kind: ErrorKind,
  pub message: String,
}

impl QCompressError {
  pub(crate) fn new<S: AsRef<str>>(kind: ErrorKind, message: S) -> Self {
    QCompressError {
      kind,
      message: message.as_ref().to_string(),
    }
  }

  pub(crate) fn compatibility<S: AsRef<str>>(message: S) -> Self {
    Self::new(ErrorKind::Compatibility, message)
  }

  pub(crate) fn corruption<S: AsRef<str>>(message: S) -> Self {
    Self::new(ErrorKind::Corruption, message)
  }

  pub(crate) fn insufficient_data<S: AsRef<str>>(message: S) -> Self {
    Self::new(ErrorKind::InsufficientData, message)
  }
  
  pub(crate) fn insufficient_data_recipe(
    name: &str,
    bits_to_read: usize,
    bit_idx: usize,
    total_bits: usize,
  ) -> Self {
    Self::insufficient_data(format!(
        "{}: cannot read {} bits at bit idx {} out of {}",
        name,
        bits_to_read,
        bit_idx,
        total_bits,
    ))
  }

  pub(crate) fn invalid_argument<S: AsRef<str>>(message: S) -> Self {
    Self::new(ErrorKind::InvalidArgument, message)
  }
}

impl Display for QCompressError {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "q_compress {:?} error: {}",
      self.kind,
      &self.message
    )
  }
}

impl Error for QCompressError {}

pub type QCompressResult<T> = Result<T, QCompressError>;

// #[derive(Clone, Debug)]
// pub(crate) struct InternalInsufficientDataError {}
// 
// impl Display for InternalInsufficientDataError {
//   fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
//     write!(f, "InsufficientDataError")
//   }
// }
