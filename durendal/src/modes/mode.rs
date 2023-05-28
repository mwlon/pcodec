use std::fmt::Debug;

use crate::bin::{BinCompressionInfo, BinDecompressionInfo};
use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::data_types::UnsignedLike;
use crate::errors::QCompressResult;
use crate::Bin;



pub trait Mode<U: UnsignedLike>: Copy + Debug + 'static {
  type Bin: ModeBin;

  fn compress_offset(&self, u: U, bin: &BinCompressionInfo<U>, writer: &mut BitWriter);

  fn make_mode_bin(bin: &Bin<U>) -> Self::Bin;
  fn make_decompression_info(bin: &Bin<U>) -> BinDecompressionInfo<Self::Bin> {
    BinDecompressionInfo {
      depth: bin.code_len,
      run_len_jumpstart: bin.run_len_jumpstart,
      mode_bin: Self::make_mode_bin(bin),
    }
  }

  fn unchecked_decompress_unsigned(
    &self,
    bin: &Self::Bin,
    reader: &mut BitReader,
  ) -> U;

  fn decompress_unsigned(
    &self,
    bin: &Self::Bin,
    reader: &mut BitReader,
  ) -> QCompressResult<U>;
}

#[derive(Clone, Copy, Debug)]
pub enum DynMode<U: UnsignedLike> {
  Classic,
  Gcd,
  FloatMult(U::Float),
}

pub trait ModeBin: Copy + Debug + Default {}
