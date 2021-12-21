// Different from compressor and decompressor configs, flags change the format
// of the .qco file.
// New flags may be added in over time in a backward-compatible way.

use crate::BitReader;
use crate::errors::{QCompressResult, QCompressError};

#[derive(Clone, Debug, Default)]
pub struct Flags {}

impl Flags {
  pub fn parse_from(reader: &mut BitReader) -> QCompressResult<Self> {
    // When we actually have flags, we'll do something more interesting.
    let byte = reader.read_bytes(1)?[0];
    if byte != 0 {
      return Err(QCompressError::CompatibilityError);
    }

    Ok(Self {})
  }

  pub fn to_bytes(&self) -> Vec<u8> {
    vec![0]
  }
}