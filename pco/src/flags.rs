// Different from compressor and decompressor configs, flags change the format
// of the compressed data.
// New flags may be added in over time in a backward-compatible way.

use crate::bit_reader::BitReader;
use crate::bit_words::PaddedBytes;
use crate::bit_writer::BitWriter;
use crate::constants::{BITS_TO_ENCODE_DELTA_ENCODING_ORDER, MAX_DELTA_ENCODING_ORDER};
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
  /// How many times delta encoding was applied during compression.
  /// This is stored as 3 bits to express 0-7.
  /// See [`CompressorConfig`][crate::CompressorConfig] for more details.
  ///
  /// Introduced in 0.0.0.
  pub delta_encoding_order: usize,
  /// Whether to the data is part of a wrapping format.
  /// This causes `pco` to omit count and compressed size metadata
  /// and also break each chunk into finer data pages.
  ///
  /// Introduced in 0.0.0.
  pub use_wrapped_mode: bool,
}

impl Flags {
  fn core_parse_from(flags: &mut Flags, reader: &mut BitReader) -> PcoResult<()> {
    flags.delta_encoding_order = reader.read_usize(BITS_TO_ENCODE_DELTA_ENCODING_ORDER)?;
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
      delta_encoding_order: 0,
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
    if self.delta_encoding_order > MAX_DELTA_ENCODING_ORDER {
      return Err(PcoError::invalid_argument(format!(
        "delta encoding order may not exceed {} (was {})",
        MAX_DELTA_ENCODING_ORDER, self.delta_encoding_order,
      )));
    }

    let start_bit_idx = writer.bit_size();
    writer.write_aligned_byte(0)?; // to later be filled with # subsequent bytes

    let pre_byte_size = writer.byte_size();

    writer.write_usize(
      self.delta_encoding_order,
      BITS_TO_ENCODE_DELTA_ENCODING_ORDER,
    );
    writer.write_one(self.use_wrapped_mode);

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

  pub(crate) fn from_config(config: &CompressorConfig, use_wrapped_mode: bool) -> Self {
    Flags {
      delta_encoding_order: config.delta_encoding_order,
      use_wrapped_mode,
    }
  }
}
