// Different from compressor and decompressor configs, flags change the format
// of the compressed data.
// New flags may be added in over time in a backward-compatible way.

use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::errors::{PcoError, PcoResult};

use crate::constants::CURRENT_FORMAT_VERSION;

/// The configuration stored in a pco header.
///
/// During compression, flags are determined based on your `CompressorConfig`
/// and the `pco` version.
/// Flags affect the encoding of the rest of the file, so decompressing with
/// the wrong flags will likely cause a corruption error.
///
/// You will not need to manually instantiate flags; that should be done
/// internally by `Compressor::from_config`.
/// However, in some circumstances you may want to inspect flags during
/// decompression.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FormatVersion(pub u8);

impl FormatVersion {
  pub(crate) fn parse_from(reader: &mut BitReader) -> PcoResult<Self> {
    let version = reader.read_aligned_bytes(1)?[0];
    if version > CURRENT_FORMAT_VERSION {
      return Err(PcoError::compatibility(format!(
        "file's format version ({}) exceeds max supported ({}); consider upgrading pco",
        version, CURRENT_FORMAT_VERSION,
      )));
    }

    Ok(Self(version))
  }

  pub(crate) fn write_to(&self, writer: &mut BitWriter) -> PcoResult<()> {
    // in the future, we may want to allow the user to encode with their choice of a recent version
    writer.write_aligned_bytes(&[self.0])
  }
}
