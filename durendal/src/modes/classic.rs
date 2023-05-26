use crate::Bin;
use crate::bin::{BinCompressionInfo, BinDecompressionInfo};
use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::data_types::{NumberLike, UnsignedLike};
use crate::errors::QCompressResult;
use crate::modes::Mode;

// formula: bin lower + offset
#[derive(Clone, Copy, Debug)]
pub struct ClassicMode;

impl<U: UnsignedLike> Mode<U> for ClassicMode {
  #[inline]
  fn compress_offset(&self, u: U, bin: &BinCompressionInfo<U>, writer: &mut BitWriter) {
    writer.write_diff(u - bin.lower, bin.offset_bits);
  }

    fn make_decompression_info(bin: &Bin<U>) -> BinDecompressionInfo<U> {
    BinDecompressionInfo {
      depth: bin.code_len,
      run_len_jumpstart: bin.run_len_jumpstart,
      unsigned0: bin.lower,
      unsigned1: U::ZERO,
      bitlen0: bin.offset_bits,
      bitlen1: 0,
    }
  }

  #[inline]
  fn unchecked_decompress_unsigned(
    &self,
    bin: &BinDecompressionInfo<U>,
    reader: &mut BitReader,
  ) -> U {
    bin.unsigned0 + reader.unchecked_read_uint::<U>(bin.bitlen0)
  }

  #[inline]
  fn decompress_unsigned(
    &self,
    bin: &BinDecompressionInfo<U>,
    reader: &mut BitReader,
  ) -> QCompressResult<U> {
    Ok(bin.unsigned0 + reader.read_uint::<U>(bin.bitlen0)?)
  }
}
