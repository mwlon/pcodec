// Different from compressor and decompressor configs, flags change the format
// of the .qco file.
// New flags may be added in over time in a backward-compatible way.

use crate::{BitReader, BitWriter};
use crate::errors::{QCompressError, QCompressResult};

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

  pub fn write(&self, writer: &mut BitWriter) -> QCompressResult<()> {
    writer.write_aligned_byte(0)
  }
}