use crate::bin::{BinCompressionInfo, BinDecompressionInfo};
use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::bits;
use crate::constants::Bitlen;
use crate::data_types::UnsignedLike;
use crate::errors::QCompressResult;
use crate::modes::classic::ClassicMode;
use crate::modes::Mode;

pub fn calc_adj_lower<U: UnsignedLike>(adj_offset_bits: Bitlen) -> U {
  if adj_offset_bits == 0 {
    U::ZERO
  } else {
    U::ZERO.wrapping_sub(U::ONE << (adj_offset_bits - 1))
  }
}

#[derive(Clone, Copy, Debug)]
pub struct AdjustedMode<U: UnsignedLike> {
  adj_bits: Bitlen,
  adj_lower: U,
}

impl<U: UnsignedLike> AdjustedMode<U> {
  pub fn new(adj_bits: Bitlen) -> Self {
    Self {
      adj_bits,
      adj_lower: calc_adj_lower::<U>(adj_bits),
    }
  }
}

impl<U: UnsignedLike> Mode<U> for AdjustedMode<U> {
  type BinOptAccumulator = ();
  fn combine_bin_opt_acc(_bin: &BinCompressionInfo<U>, _acc: &mut Self::BinOptAccumulator) {}

  fn bin_cost(&self, lower: U, upper: U, count: usize, _acc: &Self::BinOptAccumulator) -> f64 {
    let offset_bits = bits::bits_to_encode_offset(upper - lower);
    (count * (self.adj_bits + offset_bits) as usize) as f64
  }

  fn fill_optimized_compression_info(
    &self,
    acc: Self::BinOptAccumulator,
    bin: &mut BinCompressionInfo<U>,
  ) {
    ClassicMode.fill_optimized_compression_info(acc, bin);
  }

  const USES_ADJUSTMENT: bool = true;

  #[inline]
  fn compress_offset(&self, u: U, bin: &BinCompressionInfo<U>, writer: &mut BitWriter) {
    ClassicMode.compress_offset(u, bin, writer);
  }

  #[inline]
  fn compress_adjustment(&self, adjustment: U, writer: &mut BitWriter) {
    writer.write_diff(
      adjustment.wrapping_sub(self.adj_lower),
      self.adj_bits,
    );
  }

  #[inline]
  fn unchecked_decompress_unsigned(
    &self,
    bin: &BinDecompressionInfo<U>,
    reader: &mut BitReader,
  ) -> U {
    ClassicMode.unchecked_decompress_unsigned(bin, reader)
  }

  #[inline]
  fn decompress_unsigned(
    &self,
    bin: &BinDecompressionInfo<U>,
    reader: &mut BitReader,
  ) -> QCompressResult<U> {
    ClassicMode.decompress_unsigned(bin, reader)
  }

  #[inline]
  fn unchecked_decompress_adjustment(&self, reader: &mut BitReader) -> U {
    self
      .adj_lower
      .wrapping_add(reader.unchecked_read_uint(self.adj_bits))
  }

  #[inline]
  fn decompress_adjustment(&self, reader: &mut BitReader) -> QCompressResult<U> {
    Ok(
      self
        .adj_lower
        .wrapping_add(reader.read_uint(self.adj_bits)?),
    )
  }
}
