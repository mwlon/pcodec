use crate::bin::{BinCompressionInfo, BinDecompressionInfo};
use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::data_types::UnsignedLike;
use crate::errors::QCompressResult;
use crate::modes::Mode;

// formula: bin lower + offset
#[derive(Clone, Copy, Debug)]
pub struct ClassicMode;

impl<U: UnsignedLike> Mode<U> for ClassicMode {
  #[inline]
  fn compress_offset(&self, u: U, bin: BinCompressionInfo<U>, writer: &mut BitWriter) {
    writer.write_diff(u - bin.lower, bin.offset_bits);
  }

  #[inline]
  fn unchecked_decompress_unsigned(
    &self,
    bin: BinDecompressionInfo<U>,
    reader: &mut BitReader,
  ) -> U {
    bin.lower_unsigned + reader.unchecked_read_uint::<U>(bin.offset_bits)
  }

  #[inline]
  fn decompress_unsigned(
    &self,
    bin: BinDecompressionInfo<U>,
    reader: &mut BitReader,
  ) -> QCompressResult<U> {
    Ok(bin.lower_unsigned + reader.read_uint::<U>(bin.offset_bits)?)
  }
}
