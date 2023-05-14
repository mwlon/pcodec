// Different from compressor and decompressor configs, flags change the format
// of the compressed data.
// New flags may be added in over time in a backward-compatible way.

use std::convert::{TryFrom, TryInto};

use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::bits;
use crate::constants::{BITS_TO_ENCODE_DELTA_ENCODING_ORDER, MAX_DELTA_ENCODING_ORDER};
use crate::errors::{QCompressError, QCompressResult};
use crate::CompressorConfig;

/// The configuration stored in a Quantile-compressed header.
///
/// During compression, flags are determined based on your `CompressorConfig`
/// and the `q_compress` version.
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
  /// See `CompressorConfig` for more details.
  ///
  /// Introduced in 0.0.0.
  pub delta_encoding_order: usize,
  /// Whether to release control to a wrapping columnar format.
  /// This causes q_compress to omit count and compressed size metadata
  /// and also break each chuk into finer data pages.
  ///
  /// Introduced in 0.0.0.
  pub use_wrapped_mode: bool,
}

impl TryFrom<Vec<bool>> for Flags {
  type Error = QCompressError;

  fn try_from(bools: Vec<bool>) -> QCompressResult<Self> {
    let mut flags = Flags {
      delta_encoding_order: 0,
      use_wrapped_mode: false,
    };

    let mut bit_iter = bools.iter();

    let mut delta_encoding_bits = Vec::new();
    while delta_encoding_bits.len() < BITS_TO_ENCODE_DELTA_ENCODING_ORDER {
      delta_encoding_bits.push(bit_iter.next().cloned().unwrap_or(false));
    }
    flags.delta_encoding_order = bits::bits_to_usize(&delta_encoding_bits);

    flags.use_wrapped_mode = bit_iter.next() == Some(&true);

    // if we ever add another bit flag, it will increase file size by 1 byte when on

    for &bit in bit_iter {
      if bit {
        return Err(QCompressError::compatibility(
          "cannot parse flags; likely written by newer version of q_compress",
        ));
      }
    }

    Ok(flags)
  }
}

impl TryInto<Vec<bool>> for &Flags {
  type Error = QCompressError;

  fn try_into(self) -> QCompressResult<Vec<bool>> {
    let mut res = Vec::new();

    if self.delta_encoding_order > MAX_DELTA_ENCODING_ORDER {
      return Err(QCompressError::invalid_argument(format!(
        "delta encoding order may not exceed {} (was {})",
        MAX_DELTA_ENCODING_ORDER, self.delta_encoding_order,
      )));
    }
    let delta_bits = bits::usize_to_bits(
      self.delta_encoding_order,
      BITS_TO_ENCODE_DELTA_ENCODING_ORDER,
    );
    res.extend(delta_bits);

    res.push(self.use_wrapped_mode);

    let necessary_len = res
      .iter()
      .rposition(|&bit| bit)
      .map(|idx| idx + 1)
      .unwrap_or(0);
    res.truncate(necessary_len);

    Ok(res)
  }
}

impl Flags {
  pub(crate) fn parse_from(reader: &mut BitReader) -> QCompressResult<Self> {
    // assert it's byte-aligned
    let n_bytes = reader.read_aligned_bytes(1)?[0];
    let flag_bytes = reader.read_aligned_bytes(n_bytes as usize)?;
    let bools = bits::bytes_to_bits(flag_bytes);
    Self::try_from(bools)
  }

  pub(crate) fn write(&self, writer: &mut BitWriter) -> QCompressResult<()> {
    let bools: Vec<bool> = self.try_into()?;
    let bytes = bits::bits_to_bytes(bools);
    let len = bytes.len();
    if len > u8::MAX as usize {
      return Err(QCompressError::invalid_argument(
        "cannot write flags of more than 255 bytes",
      ));
    }
    writer.write_aligned_byte(len as u8)?;
    writer.write_aligned_bytes(&bytes)?;
    writer.finish_byte();

    Ok(())
  }

  pub(crate) fn check_mode(&self, expect_wrapped_mode: bool) -> QCompressResult<()> {
    if self.use_wrapped_mode != expect_wrapped_mode {
      Err(QCompressError::invalid_argument(
        "found conflicting standalone/wrapped modes between decompressor and header",
      ))
    } else {
      Ok(())
    }
  }

  pub(crate) fn bits_to_encode_count(&self, n: usize) -> usize {
    // If we use wrapped mode, we don't encode the bin counts at all (even
    // though they are nonzero). This propagates nicely through bin
    // optimization.
    if self.use_wrapped_mode {
      0
    } else {
      bits::bits_to_encode(n)
    }
  }

  pub(crate) fn from_config(config: &CompressorConfig, use_wrapped_mode: bool) -> Self {
    Flags {
      delta_encoding_order: config.delta_encoding_order,
      use_wrapped_mode,
    }
  }
}
