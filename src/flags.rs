// Different from compressor and decompressor configs, flags change the format
// of the .qco file.
// New flags may be added in over time in a backward-compatible way.

use std::convert::{TryFrom, TryInto};

use crate::{BitReader, BitWriter, CompressorConfig};
use crate::bits;
use crate::constants::{BITS_TO_ENCODE_DELTA_ENCODING_ORDER, MAX_DELTA_ENCODING_ORDER};
use crate::errors::{QCompressError, QCompressResult};

#[derive(Clone, Debug, Default)]
pub struct Flags {
  pub use_5_bit_prefix_len: bool,
  pub delta_encoding_order: usize,
}

impl TryFrom<Vec<bool>> for Flags {
  type Error = QCompressError;

  fn try_from(bools: Vec<bool>) -> QCompressResult<Self> {
    // would be nice to make a bit reader to do this instead of keeping track of index manually
    let use_5_bit_prefix_len = bools[0];
    let delta_end_idx = 1 + BITS_TO_ENCODE_DELTA_ENCODING_ORDER as usize;
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
        "delta encoding level may not exceed {} (was {})",
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
    reader.aligned_byte_ind()?; // assert it's byte-aligned
    let mut bools = Vec::new();
    loop {
      bools.extend(reader.read(7));
      if !reader.read_one() {
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
      let end = (start + 7).min(bools.len());
      writer.write_bits(&bools[start..end]);
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
