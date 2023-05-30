use std::fmt::Debug;

use crate::bin::{BinCompressionInfo, BinDecompressionInfo};
use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::data_types::UnsignedLike;
use crate::errors::QCompressResult;
use crate::Bin;

pub trait Mode<U: UnsignedLike>: Copy + Debug + 'static {
  // BIN OPTIMIZATION
  const EXTRA_META_COST: f64 = 0.0;
  type BinOptAccumulator: Default;
  fn combine_bin_opt_acc(bin: &BinCompressionInfo<U>, acc: &mut Self::BinOptAccumulator);
  fn bin_cost(&self, lower: U, upper: U, count: usize, acc: &Self::BinOptAccumulator) -> f64;
  fn fill_optimized_compression_info(
    &self,
    acc: Self::BinOptAccumulator,
    bin: &mut BinCompressionInfo<U>,
  );

  // COMPRESSION
  fn compress_offset(&self, u: U, bin: &BinCompressionInfo<U>, writer: &mut BitWriter);

  // DECOMPRESSION
  type Bin: ModeBin;
  fn make_mode_bin(bin: &Bin<U>) -> Self::Bin;
  fn make_decompression_info(bin: &Bin<U>) -> BinDecompressionInfo<Self::Bin> {
    BinDecompressionInfo {
      depth: bin.code_len,
      run_len_jumpstart: bin.run_len_jumpstart,
      mode_bin: Self::make_mode_bin(bin),
    }
  }
  fn unchecked_decompress_unsigned(&self, bin: &Self::Bin, reader: &mut BitReader) -> U;
  fn decompress_unsigned(&self, bin: &Self::Bin, reader: &mut BitReader) -> QCompressResult<U>;
}

#[derive(Clone, Copy, Debug)]
pub enum DynMode<U: UnsignedLike> {
  Classic,
  Gcd,
  FloatMult { base: U::Float, inv_base: U::Float },
}

pub trait ModeBin: Copy + Debug + Default {}
