// Different from compressor and decompressor configs, flags change the format
// of the compressed data.
// New flags may be added in over time in a backward-compatible way.

use crate::bit_reader::BitReader;
use crate::bit_words::PaddedBytes;
use crate::bit_writer::BitWriter;
use crate::errors::{ErrorKind, PcoError, PcoResult};
use crate::CompressorConfig;
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormatVersion(pub u8);

impl FormatVersion {
  pub(crate) fn parse_from(reader: &mut BitReader) -> PcoResult<Self> {
    let version = reader.read_aligned_bytes(1)?[0];
    if version > CURRENT_FORMAT_VERSION {
      return Err(PcoError::compatibility(format!(
        "file's format version ({}) exceeds max supported ({}); consider upgrading pco",
        version,
        CURRENT_FORMAT_VERSION,
      )))
    }

    Ok(Self(version))
    // let n_bytes = reader.read_aligned_bytes(1)?[0] as usize;
    // let bytes = reader.read_aligned_bytes(n_bytes)?;
    // let sub_bit_words = PaddedBytes::from(bytes);
    // let mut sub_reader = BitReader::from(&sub_bit_words);
    //
    // let mut flags = FormatVersion {
    //   use_wrapped_mode: false,
    // };
    // let parse_res = Self::core_parse_from(&mut flags, &mut sub_reader);
    // match parse_res {
    //   Ok(()) => Ok(flags),
    //   Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => Ok(flags),
    //   Err(e) => Err(e),
    // }
  }

  pub(crate) fn write_to(&self, writer: &mut BitWriter) -> PcoResult<()> {
    // in the future, we may want to allow the user to encode with their choice of a recent version
    writer.write_aligned_byte(CURRENT_FORMAT_VERSION as u8)?;
    // let start_bit_idx = writer.bit_size();
    // writer.write_aligned_byte(0)?; // to later be filled with # subsequent bytes
    //
    // let pre_byte_size = writer.byte_size();
    //
    // // write each flags here
    // writer.write_one(self.use_wrapped_mode);
    // // done writing each flag
    //
    // writer.finish_byte();
    // let byte_len = writer.byte_size() - pre_byte_size;
    //
    // if byte_len > u8::MAX as usize {
    //   return Err(PcoError::invalid_argument(
    //     "cannot write flags of more than 255 bytes",
    //   ));
    // }
    //
    // writer.overwrite_usize(start_bit_idx, byte_len, 8);
    //
    Ok(())
  }
}
