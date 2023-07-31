use crate::ans::Token;
use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::bits::bits_to_encode_offset_bits;

use crate::constants::{Bitlen, Weight};
use crate::data_types::UnsignedLike;
use crate::errors::{PcoError, PcoResult};
use crate::Mode;

/// Part of [`ChunkStreamMetadata`][`crate::ChunkStreamMetadata`] representing
/// a numerical range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Bin<U: UnsignedLike> {
  /// The number of occurrences of this bin in the asymmetric numeral system
  /// table.
  pub weight: Weight,
  /// The lower bound for this bin's numerical range.
  pub lower: U,
  /// The log of the size of this bin's (inclusive) numerical range.
  pub offset_bits: Bitlen,
  /// The greatest common divisor of all numbers belonging to this bin
  /// (in the data type's corresponding unsigned integer).
  pub gcd: U,
}

impl<U: UnsignedLike> Bin<U> {
  pub(crate) fn write_to(&self, mode: Mode<U>, ans_size_log: Bitlen, writer: &mut BitWriter) {
    writer.write_diff(self.weight - 1, ans_size_log);
    writer.write_diff(self.lower, U::BITS);
    writer.write_bitlen(
      self.offset_bits,
      bits_to_encode_offset_bits::<U>(),
    );

    match mode {
      Mode::Classic => (),
      Mode::Gcd => {
        if self.offset_bits > 0 {
          writer.write_diff(self.gcd, U::BITS);
        }
      }
      Mode::FloatMult { .. } => (),
    }
  }

  pub(crate) fn parse_from(
    reader: &mut BitReader,
    mode: Mode<U>,
    ans_size_log: Bitlen,
  ) -> PcoResult<Self> {
    let weight = reader.read_uint::<Weight>(ans_size_log)? + 1;
    let lower = reader.read_uint::<U>(U::BITS)?;

    let offset_bits = reader.read_bitlen(bits_to_encode_offset_bits::<U>())?;
    if offset_bits > U::BITS {
      return Err(PcoError::corruption(format!(
        "offset bits of {} exceeds data type of {} bits",
        offset_bits,
        U::BITS,
      )));
    }

    let gcd = match mode {
      Mode::Gcd if offset_bits != 0 => reader.read_uint(U::BITS)?,
      _ => U::ONE,
    };

    Ok(Bin {
      weight,
      lower,
      offset_bits,
      gcd,
    })
  }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BinCompressionInfo<U: UnsignedLike> {
  pub weight: Weight,
  pub lower: U,
  pub upper: U,
  pub offset_bits: Bitlen,
  pub gcd: U,
  // token is also the index of this in the list of optimized compression infos
  pub token: Token,
}

impl<U: UnsignedLike> From<BinCompressionInfo<U>> for Bin<U> {
  fn from(info: BinCompressionInfo<U>) -> Self {
    Bin {
      weight: info.weight,
      lower: info.lower,
      offset_bits: info.offset_bits,
      gcd: info.gcd,
    }
  }
}

impl<U: UnsignedLike> BinCompressionInfo<U> {
  pub fn contains(&self, unsigned: U) -> bool {
    self.lower <= unsigned && self.upper >= unsigned
  }
}

impl<U: UnsignedLike> Default for BinCompressionInfo<U> {
  fn default() -> Self {
    BinCompressionInfo {
      weight: 0,
      lower: U::ZERO,
      upper: U::MAX,
      offset_bits: U::BITS,
      gcd: U::ONE,
      token: Token::MAX,
    }
  }
}

// Default here is meaningless and should only be used to fill in empty
// vectors.
#[derive(Clone, Copy, Debug, Default)]
pub struct BinDecompressionInfo<U: UnsignedLike> {
  pub lower: U,
  pub offset_bits: Bitlen,
  pub gcd: U,
}

impl<U: UnsignedLike> From<&Bin<U>> for BinDecompressionInfo<U> {
  fn from(bin: &Bin<U>) -> Self {
    Self {
      lower: bin.lower,
      offset_bits: bin.offset_bits,
      gcd: bin.gcd,
    }
  }
}

pub(crate) fn bins_are_trivial<U: UnsignedLike>(bins: &[Bin<U>]) -> bool {
  bins.is_empty() || (bins.len() == 1 && bins[0].offset_bits == 0)
}
