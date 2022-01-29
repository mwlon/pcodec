use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
  Compatibility,
  Corruption,
  InvalidArgument,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QCompressError {
  pub kind: ErrorKind,
  pub message: String,
}

impl QCompressError {
  pub fn new(kind: ErrorKind, message: impl AsRef<str>) -> Self {
    QCompressError {
      kind,
      message: message.as_ref().to_string(),
    }
  }

  pub fn compatibility(message: impl AsRef<str>) -> Self {
    Self::new(ErrorKind::Compatibility, message)
  }

  pub fn corruption(message: impl AsRef<str>) -> Self {
    Self::new(ErrorKind::Corruption, message)
  }

  pub fn invalid_argument(message: impl AsRef<str>) -> Self {
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
