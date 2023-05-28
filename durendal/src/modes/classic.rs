use crate::bin::{BinCompressionInfo};
use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::data_types::UnsignedLike;
use crate::errors::QCompressResult;
use crate::modes::{Mode};
use crate::Bin;
use crate::modes::gcd::GcdBin;

// formula: bin lower + offset
#[derive(Clone, Copy, Debug)]
pub struct ClassicMode;

impl<U: UnsignedLike> Mode<U> for ClassicMode {
  // GcdBin is a superset of what we need;
  // no apparent performance harm from reusing it.
  type Bin = GcdBin<U>;

  #[inline]
  fn compress_offset(&self, u: U, bin: &BinCompressionInfo<U>, writer: &mut BitWriter) {
    writer.write_diff(u - bin.lower, bin.offset_bits);
  }

  fn make_mode_bin(bin: &Bin<U>) -> GcdBin<U> {
    GcdBin {
      lower: bin.lower,
      gcd: U::ZERO,
      offset_bits: bin.offset_bits,
    }
  }

  #[inline]
  fn unchecked_decompress_unsigned(
    &self,
    bin: &GcdBin<U>,
    reader: &mut BitReader,
  ) -> U {
    bin.lower + reader.unchecked_read_uint::<U>(bin.offset_bits)
  }

  #[inline]
  fn decompress_unsigned(
    &self,
    bin: &GcdBin<U>,
    reader: &mut BitReader,
  ) -> QCompressResult<U> {
    Ok(bin.lower + reader.read_uint::<U>(bin.offset_bits)?)
  }
}
