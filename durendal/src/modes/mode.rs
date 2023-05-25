use std::fmt::Debug;
use crate::bin::{BinCompressionInfo, BinDecompressionInfo};
use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::data_types::UnsignedLike;
use crate::errors::QCompressResult;

pub trait Mode<U: UnsignedLike>: Copy + Debug {
  fn compress_offset(&self, u: U, bin: BinCompressionInfo<U>, writer: &mut BitWriter);
  fn unchecked_decompress_unsigned(&self, bin: BinDecompressionInfo<U>, reader: &mut BitReader) -> U;
  fn decompress_unsigned(&self, bin: BinDecompressionInfo<U>, reader: &mut BitReader) -> QCompressResult<U>;
}

#[derive(Clone, Copy, Debug)]
pub enum DynMode {
  Classic,
  Gcd,
}
