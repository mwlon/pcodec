use crate::Bin;
use crate::bin::{BinCompressionInfo, BinDecompressionInfo};
use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;

use crate::data_types::{FloatLike, NumberLike, UnsignedLike};
use crate::errors::QCompressResult;
use crate::modes::Mode;

#[derive(Clone, Copy, Debug)]
pub struct FloatMultMode<U: UnsignedLike> {
  ratio: U::Float,
  inv_ratio: U::Float,
}

impl<U: UnsignedLike> FloatMultMode<U> {
  pub fn new(ratio: U::Float) -> Self {
    Self {
      ratio,
      inv_ratio: ratio.inv()
    }
  }
}

impl<U: UnsignedLike> Mode<U> for FloatMultMode<U> {
  #[inline]
  fn compress_offset(&self, u: U, bin: &BinCompressionInfo<U>, writer: &mut BitWriter) {
    let mult = (u.to_float() * self.inv_ratio).round();
    // note that
    writer.write_diff(mult.to_unsigned() - bin.float_mult_base, bin.offset_bits);
    let approx = U::from_float_bits(mult * self.ratio);
    let adj = u.wrapping_sub(approx).wrapping_sub(bin.adj_base);
    writer.write_diff(adj, bin.adj_bits);
  }

  fn make_decompression_info(bin: &Bin<U>) -> BinDecompressionInfo<U> {
    BinDecompressionInfo {
      depth: bin.code_len,
      run_len_jumpstart: bin.run_len_jumpstart,
      unsigned0: bin.lower,
      unsigned1: bin.adj_base,
      bitlen0: bin.offset_bits,
      bitlen1: bin.adj_bits,
    }
  }

  #[inline]
  fn unchecked_decompress_unsigned(
    &self,
    bin: &BinDecompressionInfo<U>,
    reader: &mut BitReader,
  ) -> U {
    let mult = bin.unsigned0.wrapping_add(reader.unchecked_read_uint::<U>(bin.bitlen0));
    let approx = U::from_float_bits(mult.to_float() * self.ratio);
    approx.wrapping_add(bin.unsigned1).wrapping_add(reader.unchecked_read_uint::<U>(bin.bitlen1))
  }

  #[inline]
  fn decompress_unsigned(
    &self,
    bin: &BinDecompressionInfo<U>,
    reader: &mut BitReader,
  ) -> QCompressResult<U> {
    let mult = bin.unsigned0.wrapping_add(reader.read_uint::<U>(bin.bitlen0)?);
    let approx = U::from_float_bits(mult.to_float() * self.ratio);
    Ok(approx.wrapping_add(bin.unsigned1).wrapping_add(reader.read_uint::<U>(bin.bitlen1)?))
  }
}