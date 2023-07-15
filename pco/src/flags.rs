// Different from compressor and decompressor configs, flags change the format
// of the compressed data.
// New flags may be added in over time in a backward-compatible way.

use crate::bit_reader::BitReader;
use crate::bit_words::PaddedBytes;
use crate::bit_writer::BitWriter;
use crate::errors::{ErrorKind, PcoError, PcoResult};
use crate::CompressorConfig;

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
#[non_exhaustive]
pub struct Flags {
  /// Whether to the data is part of a wrapping format.
  /// This causes `pco` to omit count and compressed size metadata
  /// and also break each chunk into finer data pages.
  ///
  /// Introduced in 0.0.0.
  pub use_wrapped_mode: bool,
}

impl Flags {
  fn core_parse_from(flags: &mut Flags, reader: &mut BitReader) -> PcoResult<()> {
    flags.use_wrapped_mode = reader.read_one()?;

    let compat_err =
      PcoError::compatibility("cannot parse flags; likely written by newer version of pco");
    reader
      .drain_empty_byte("")
      .map_err(|_| compat_err.clone())?;

    let remaining_bytes = reader.read_aligned_bytes(reader.bits_remaining() / 8)?;
    if remaining_bytes.iter().all(|&byte| byte == 0) {
      Ok(())
    } else {
      Err(compat_err)
    }
  }

  pub(crate) fn parse_from(reader: &mut BitReader) -> PcoResult<Self> {
    let n_bytes = reader.read_aligned_bytes(1)?[0] as usize;
    let bytes = reader.read_aligned_bytes(n_bytes)?;
    let sub_bit_words = PaddedBytes::from(bytes);
    let mut sub_reader = BitReader::from(&sub_bit_words);

    let mut flags = Flags {
      use_wrapped_mode: false,
    };
    let parse_res = Self::core_parse_from(&mut flags, &mut sub_reader);
    match parse_res {
      Ok(()) => Ok(flags),
      Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => Ok(flags),
      Err(e) => Err(e),
    }
  }

  pub(crate) fn write_to(&self, writer: &mut BitWriter) -> PcoResult<()> {
    let start_bit_idx = writer.bit_size();
    writer.write_aligned_byte(0)?; // to later be filled with # subsequent bytes

    let pre_byte_size = writer.byte_size();

    // write each flags here
    writer.write_one(self.use_wrapped_mode);
    // done writing each flag

    writer.finish_byte();
    let byte_len = writer.byte_size() - pre_byte_size;

    if byte_len > u8::MAX as usize {
      return Err(PcoError::invalid_argument(
        "cannot write flags of more than 255 bytes",
      ));
    }

    writer.overwrite_usize(start_bit_idx, byte_len, 8);

    Ok(())
  }

  pub(crate) fn check_mode(&self, expect_wrapped_mode: bool) -> PcoResult<()> {
    if self.use_wrapped_mode != expect_wrapped_mode {
      Err(PcoError::invalid_argument(
        "found conflicting standalone/wrapped modes between decompressor and header",
      ))
    } else {
      Ok(())
    }
  }

  pub(crate) fn from_config(_config: &CompressorConfig, use_wrapped_mode: bool) -> Self {
    Flags { use_wrapped_mode }
  }
}
