use std::fmt::Debug;

use crate::bin::{BinCompressionInfo, BinDecompressionInfo};
use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::constants::Bitlen;
use crate::data_types::{NumberLike, UnsignedLike};
use crate::errors::QCompressResult;
use crate::float_mult_utils;
use crate::float_mult_utils::FloatMultConfig;
use crate::unsigned_src_dst::{UnsignedDst, StreamSrc};

// Static, compile-time modes. Logic should go here if it's called in hot
// loops.
pub trait Mode<U: UnsignedLike>: Copy + Debug + 'static {
  // BIN OPTIMIZATION
  type BinOptAccumulator: Default;
  fn combine_bin_opt_acc(bin: &BinCompressionInfo<U>, acc: &mut Self::BinOptAccumulator);
  fn bin_cost(&self, lower: U, upper: U, count: usize, acc: &Self::BinOptAccumulator) -> f64;
  fn fill_optimized_compression_info(
    &self,
    acc: Self::BinOptAccumulator,
    bin: &mut BinCompressionInfo<U>,
  );

  // COMPRESSION
  fn calc_offset(
    u: U,
    bin: &BinCompressionInfo<U>,
  ) -> U;

  // DECOMPRESSION
  fn unchecked_decompress_unsigned(
    bin: &BinDecompressionInfo<U>,
    reader: &mut BitReader,
  ) -> U;
  fn decompress_unsigned(
    bin: &BinDecompressionInfo<U>,
    reader: &mut BitReader,
  ) -> QCompressResult<U>;
}

// Dynamic modes. Logic should go here if it isn't called in hot loops.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DynMode<U: UnsignedLike> {
  #[default]
  Classic,
  Gcd,
  FloatMult {
    inv_base: U::Float,
    base: U::Float,
  },
}

impl<U: UnsignedLike> DynMode<U> {
  pub fn float_mult(config: FloatMultConfig<U::Float>) -> Self {
    Self::FloatMult {
      inv_base: config.inv_base,
      base: config.base,
    }
  }

  pub fn n_streams(&self) -> usize {
    match self {
      DynMode::Classic | DynMode::Gcd => 1,
      DynMode::FloatMult { .. } => 2,
    }
  }

  pub fn stream_delta_order(&self, stream_idx: usize, delta_order: usize) -> usize {
    match (self, stream_idx) {
      (DynMode::Classic, 0) => delta_order,
      (DynMode::Gcd, 0) => delta_order,
      (DynMode::FloatMult { .. }, 0) => delta_order,
      (DynMode::FloatMult { .. }, 1) => 0,
      _ => panic!("should be unreachable; unknown stream {:?}/{}", self, stream_idx),
    }
  }
}
