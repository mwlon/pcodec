use crate::bin::BinDecompressionInfo;
use crate::bit_reader::BitReader;
use crate::data_types::UnsignedLike;
use crate::modes::Mode;
use crate::{num_decompressor, Bin};

pub fn use_run_len<U: UnsignedLike>(bins: &[Bin<U>]) -> bool {
  bins.iter().any(|p| p.run_len_jumpstart.is_some())
}

pub trait RunLenOperator {
  // returns count of numbers processed
  fn unchecked_decompress_for_bin<U: UnsignedLike, M: Mode<U>>(
    state: &mut num_decompressor::State<U>,
    reader: &mut BitReader,
    bin: &BinDecompressionInfo<U>,
    mode: M,
    dest: &mut [U],
  ) -> usize;

  fn batch_ongoing(len: usize, batch_size: usize) -> bool;
}

pub struct GeneralRunLenOp;

impl RunLenOperator for GeneralRunLenOp {
  #[inline]
  fn unchecked_decompress_for_bin<U: UnsignedLike, M: Mode<U>>(
    state: &mut num_decompressor::State<U>,
    reader: &mut BitReader,
    bin: &BinDecompressionInfo<U>,
    mode: M,
    dest: &mut [U],
  ) -> usize {
    match bin.run_len_jumpstart {
      None => {
        dest[0] = mode.unchecked_decompress_unsigned(bin, reader);
        1
      }
      // we stored the number of occurrences minus 1 because we knew it's at least 1
      Some(jumpstart) => {
        let full_reps = reader.unchecked_read_varint(jumpstart) + 1;
        let reps = state.unchecked_limit_reps(*bin, full_reps, dest.len());
        for i in 0..reps {
          dest[i] = mode.unchecked_decompress_unsigned(bin, reader);
        }
        reps
      }
    }
  }

  #[inline]
  fn batch_ongoing(len: usize, batch_size: usize) -> bool {
    len < batch_size
  }
}

pub struct TrivialRunLenOp;

impl RunLenOperator for TrivialRunLenOp {
  #[inline]
  fn unchecked_decompress_for_bin<U: UnsignedLike, M: Mode<U>>(
    _state: &mut num_decompressor::State<U>,
    reader: &mut BitReader,
    bin: &BinDecompressionInfo<U>,
    mode: M,
    dest: &mut [U],
  ) -> usize {
    dest[0] = mode.unchecked_decompress_unsigned(bin, reader);
    1
  }

  #[inline]
  fn batch_ongoing(_len: usize, _batch_size: usize) -> bool {
    true
  }
}
