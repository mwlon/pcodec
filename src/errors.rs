use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
  Compatibility,
  Corruption,
  InsufficientData,
  InvalidArgument,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QCompressError {
  pub kind: ErrorKind,
  pub message: String,
}

impl QCompressError {
  pub fn new<S: AsRef<str>>(kind: ErrorKind, message: S) -> Self {
    QCompressError {
      kind,
      message: message.as_ref().to_string(),
    }
  }

  pub fn compatibility<S: AsRef<str>>(message: S) -> Self {
    Self::new(ErrorKind::Compatibility, message)
  }

  pub fn corruption<S: AsRef<str>>(message: S) -> Self {
    Self::new(ErrorKind::Corruption, message)
  }

  pub fn insufficient_data<S: AsRef<str>>(message: S) -> Self {
    Self::new(ErrorKind::InsufficientData, message)
  }

  pub fn invalid_argument<S: AsRef<str>>(message: S) -> Self {
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
