// Uerent from compressor and decompressor configs, flags change the format
// of the .qco file.
// New flags may be added in over time in a backward-compatible way.

use std::cmp::min;
use std::convert::{TryFrom, TryInto};
use std::marker::PhantomData;

use crate::bit_reader::BitReader;
use crate::{CompressorConfig};
use crate::bit_writer::BitWriter;
use crate::bits;
use crate::constants::{BITS_TO_ENCODE_DELTA_ENCODING_ORDER, BITS_TO_ENCODE_N_ENTRIES, MAX_DELTA_ENCODING_ORDER};
use crate::errors::{QCompressError, QCompressResult};

/// The configuration stored in a .qco file's header.
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
#[derive(Clone, Debug, PartialEq)]
pub struct Flags {
  /// Whether to use 5 bits to encode the length of a prefix Huffman code,
  /// as opposed to 4.
  /// The first versions of `q_compress` used 4, which was insufficient for
  /// Huffman codes that could reach up to 23 in length
  /// (23 >= 16 = 2^4)
  /// in spiky distributions with high compression level.
  /// In later versions, this flag is always true.
  ///
  /// Introduced in 0.5.0.
  pub use_5_bit_code_len: bool,
  /// How many times delta encoding was applied during compression.
  /// This is stored as 3 bits to express 0-7.
  /// See `CompressorConfig` for more details.
  ///
  /// Introduced in 0.6.0.
  pub delta_encoding_order: usize,
  /// Whether to use the minimum number of bits to encode the count of each
  /// prefix, rather than using a constant number of bits.
  /// This can reduce file size slightly for small data.
  /// In later versions, this flag is always true.
  ///
  /// Introduced in 0.9.1.
  pub use_min_count_encoding: bool,
  /// Whether to enable greatest common divisor multipliers for each
  /// prefix.
  /// This adds an optional multiplier to each prefix metadata, so that each
  /// unsigned number is decoded as `x = prefix_lower + offset * gcd`.
  /// This can improve compression ratio in some cases, e.g. when the
  /// numbers are all integer multiples of 100 or all integer-valued floats.
  ///
  /// Introduced in 0.10.0.
  pub use_gcds: bool,
  // Make it API-stable to add more fields in the future
  pub(crate) phantom: PhantomData<()>,
}

impl TryFrom<Vec<bool>> for Flags {
  type Error = QCompressError;

  fn try_from(bools: Vec<bool>) -> QCompressResult<Self> {
    let mut flags = Flags {
      use_5_bit_code_len: false,
      delta_encoding_order: 0,
      use_min_count_encoding: false,
      use_gcds: false,
      phantom: PhantomData,
    };

    let mut bit_iter = bools.iter();
    flags.use_5_bit_code_len = bit_iter.next() == Some(&true);

    let mut delta_encoding_bits = Vec::new();
    while delta_encoding_bits.len() < BITS_TO_ENCODE_DELTA_ENCODING_ORDER {
      delta_encoding_bits.push(bit_iter.next().cloned().unwrap_or(false));
    }
    flags.delta_encoding_order = bits::bits_to_usize(&delta_encoding_bits);

    flags.use_min_count_encoding = bit_iter.next() == Some(&true);

    flags.use_gcds = bit_iter.next() == Some(&true);

    for &bit in bit_iter {
      if bit {
        return Err(QCompressError::compatibility(
          "cannot parse flags; likely written by newer version of q_compress"
        ));
      }
    }

    Ok(flags)
  }
}

impl TryInto<Vec<bool>> for &Flags {
  type Error = QCompressError;

  fn try_into(self) -> QCompressResult<Vec<bool>> {
    let mut res = vec![self.use_5_bit_code_len];

    if self.delta_encoding_order > MAX_DELTA_ENCODING_ORDER {
      return Err(QCompressError::invalid_argument(format!(
        "delta encoding order may not exceed {} (was {})",
        MAX_DELTA_ENCODING_ORDER,
        self.delta_encoding_order,
      )));
    }
    let delta_bits = bits::usize_truncated_to_bits(self.delta_encoding_order, BITS_TO_ENCODE_DELTA_ENCODING_ORDER);
    res.extend(delta_bits);

    res.push(self.use_min_count_encoding);

    res.push(self.use_gcds);

    let necessary_len = res.iter()
      .rposition(|&bit| bit)
      .map(|idx| idx + 1)
      .unwrap_or(0);
    res.truncate(necessary_len);

    Ok(res)
  }
}

impl Flags {
  pub(crate) fn parse_from(reader: &mut BitReader) -> QCompressResult<Self> {
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

  pub(crate) fn write(&self, writer: &mut BitWriter) -> QCompressResult<()> {
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


  pub(crate) fn bits_to_encode_code_len(&self) -> usize {
    if self.use_5_bit_code_len {
      5
    } else {
      4
    }
  }

  pub(crate) fn bits_to_encode_count(&self, n: usize) -> usize {
    if self.use_min_count_encoding {
      ((n + 1) as f64).log2().ceil() as usize
    } else {
      BITS_TO_ENCODE_N_ENTRIES
    }
  }
}

impl From<&CompressorConfig> for Flags {
  fn from(config: &CompressorConfig) -> Self {
    Flags {
      use_5_bit_code_len: true,
      delta_encoding_order: config.delta_encoding_order,
      use_min_count_encoding: true,
      use_gcds: config.use_gcds,
      phantom: PhantomData,
    }
  }
}
