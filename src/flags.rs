// Different from compressor and decompressor configs, flags change the format
// of the .qco file.
// New flags may be added in over time in a backward-compatible way.

use crate::{BitReader, BitWriter, CompressorConfig};
use crate::errors::{QCompressError, QCompressResult};

#[derive(Clone, Debug)]
pub struct Flags {
  pub use_5_bit_prefix_len: bool,
}

impl Default for Flags {
  fn default() -> Self {
    Flags {
      use_5_bit_prefix_len: true,
    }
  }
}

impl Flags {
  pub fn parse_from(reader: &mut BitReader) -> QCompressResult<Self> {
    let use_5_bit_prefix_len = reader.read_one();
    for _ in 1..8 {
      if reader.read_one() {
        return Err(QCompressError::compatibility(
          "cannot parse flags; likely written by newer version of q_compress"
        ));
      }
    }

    Ok(Self {
      use_5_bit_prefix_len,
    })
  }

  pub fn write(&self, writer: &mut BitWriter) -> QCompressResult<()> {
    writer.write_one(self.use_5_bit_prefix_len);
    writer.finish_byte();
    Ok(())
  }
}

impl From<&CompressorConfig> for Flags {
  fn from(_: &CompressorConfig) -> Self {
    // eventually we'll probably have some parts of compressor config
    // that end up in flags
    Flags::default()
  }
}
