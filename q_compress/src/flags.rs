// Different from compressor and decompressor configs, flags change the format
// of the .qco file.
// New flags may be added in over time in a backward-compatible way.

use std::cmp::min;
use std::convert::{TryFrom, TryInto};

use crate::{BitReader, BitWriter, CompressorConfig};
use crate::bits;
use crate::constants::{BITS_TO_ENCODE_DELTA_ENCODING_ORDER, MAX_DELTA_ENCODING_ORDER};
use crate::errors::{QCompressError, QCompressResult};

/// The configuration stored in a .qco file's header.
///
/// During compression, flags are determined based on your `CompressorConfig`
/// and the `q_compress` version.
/// Flags affect the encoding of the rest of the file, so decompressing with
/// the wrong flags will likely cause a corruption error.
///
/// Most users will not need to manually instantiate flags; that should be done
/// internally by `Compressor::from_config`.
/// However, in some circumstances you may want to inspect flags during
/// decompression.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Flags {
  /// Whether to use 5 bits to encode the length of a prefix,
  /// as opposed to 4.
  /// Earlier versions of `q_compress` used 4, which was insufficient for
  /// Huffman prefixes that could reach up to 23 in length
  /// (23 >= 16 = 2^4)
  /// in spiky distributions with high compression level.
  /// In later versions, this flag is always true.
  pub use_5_bit_prefix_len: bool,
  /// How many times delta encoding was applied during compression.
  /// This is stored as 3 bits to express 0-7
  /// See `CompressorConfig` for more details.
  pub delta_encoding_order: usize,
}

impl TryFrom<Vec<bool>> for Flags {
  type Error = QCompressError;

  fn try_from(bools: Vec<bool>) -> QCompressResult<Self> {
    // would be nice to make a bit reader to do this instead of keeping track of index manually
    let use_5_bit_prefix_len = bools[0];
    let delta_end_idx = 1 + BITS_TO_ENCODE_DELTA_ENCODING_ORDER;
    let delta_encoding_bits = &bools[1..delta_end_idx];
    let delta_encoding_order = bits::bits_to_usize(delta_encoding_bits);
    for &bit in bools.iter().skip(delta_end_idx) {
      if bit {
        return Err(QCompressError::compatibility(
          "cannot parse flags; likely written by newer version of q_compress"
        ));
      }
    }

    Ok(Self {
      use_5_bit_prefix_len,
      delta_encoding_order,
    })
  }
}

impl TryInto<Vec<bool>> for &Flags {
  type Error = QCompressError;

  fn try_into(self) -> QCompressResult<Vec<bool>> {
    let mut res = vec![self.use_5_bit_prefix_len];
    if self.delta_encoding_order > MAX_DELTA_ENCODING_ORDER {
      return Err(QCompressError::invalid_argument(format!(
        "delta encoding order may not exceed {} (was {})",
        MAX_DELTA_ENCODING_ORDER,
        self.delta_encoding_order,
      )));
    }
    let delta_bits = bits::usize_truncated_to_bits(self.delta_encoding_order, BITS_TO_ENCODE_DELTA_ENCODING_ORDER);
    res.extend(delta_bits);
    Ok(res)
  }
}

impl Flags {
  pub fn parse_from(reader: &mut BitReader) -> QCompressResult<Self> {
    reader.aligned_byte_idx()?; // assert it's byte-aligned
    let mut bools = Vec::new();
    loop {
      bools.extend(reader.read(7)?);
      if !reader.read_one()? {
        break;
      }
    }
    Self::try_from(bools)
  }

  pub fn write(&self, writer: &mut BitWriter) -> QCompressResult<()> {
    let bools: Vec<bool> = self.try_into()?;

    // reserve 1 bit at the end of every byte for whether there is a following
    // byte
    for i in 0_usize..(bools.len() / 7) + 1 {
      let start = i * 7;
      let end = min(start + 7, bools.len());
      writer.write(&bools[start..end]);
      if end < bools.len() {
        writer.write_one(true);
      }
    }
    writer.finish_byte();
    Ok(())
  }


  pub fn bits_to_encode_prefix_len(&self) -> usize {
    if self.use_5_bit_prefix_len {
      5
    } else {
      4
    }
  }
}

impl From<&CompressorConfig> for Flags {
  fn from(config: &CompressorConfig) -> Self {
    Flags {
      use_5_bit_prefix_len: true,
      delta_encoding_order: config.delta_encoding_order,
    }
  }
}
