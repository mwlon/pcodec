use std::cmp::max;
use crate::bin::BinCompressionInfo;
use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::data_types::UnsignedLike;
use crate::errors::QCompressResult;
use crate::modes::gcd::GcdBin;
use crate::modes::Mode;
use crate::{Bin, bits};
use crate::bits::avg_offset_bits;

// formula: bin lower + offset
#[derive(Clone, Copy, Debug)]
pub struct ClassicMode;

impl<U: UnsignedLike> Mode<U> for ClassicMode {
  type BinOptAccumulator = ();
  fn combine_bin_opt_acc(_bin: &BinCompressionInfo<U>, _acc: &mut Self::BinOptAccumulator) {}

  fn bin_cost(&self, lower: U, upper: U, count: usize, _acc: &Self::BinOptAccumulator) {
    avg_offset_bits(lower, upper, U::ONE) * count
  }
  fn fill_optimized_compression_info(&self, acc: Self::BinOptAccumulator, bin: &mut BinCompressionInfo<U>) {
    let max_offset = (bin.upper - bin.lower);
    bin.offset_bits = bits::bits_to_encode_offset(max_offset);
  }

  #[inline]
  fn compress_offset(&self, u: U, bin: &BinCompressionInfo<U>, writer: &mut BitWriter) {
    writer.write_diff(u - bin.lower, bin.offset_bits);
  }

  // GcdBin is a superset of what we need;
  // no apparent performance harm from reusing it.
  type Bin = GcdBin<U>;

  fn make_mode_bin(bin: &Bin<U>) -> GcdBin<U> {
    GcdBin {
      lower: bin.lower,
      gcd: U::ZERO,
      offset_bits: bin.offset_bits,
    }
  }

  #[inline]
  fn unchecked_decompress_unsigned(&self, bin: &GcdBin<U>, reader: &mut BitReader) -> U {
    bin.lower + reader.unchecked_read_uint::<U>(bin.offset_bits)
  }

  #[inline]
  fn decompress_unsigned(&self, bin: &GcdBin<U>, reader: &mut BitReader) -> QCompressResult<U> {
    Ok(bin.lower + reader.read_uint::<U>(bin.offset_bits)?)
  }
}
