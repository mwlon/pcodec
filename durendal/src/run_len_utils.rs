use std::cmp::min;

use crate::{Bin, num_decompressor};
use crate::bin::BinDecompressionInfo;
use crate::bit_reader::BitReader;
use crate::constants::{Bitlen, MAX_JUMPSTART, MIN_FREQUENCY_TO_USE_RUN_LEN, MIN_N_TO_USE_RUN_LEN};
use crate::data_types::UnsignedLike;
use crate::modes::Mode;

fn bin_needs_run_len(count: usize, n: usize, freq: f64) -> bool {
  n >= MIN_N_TO_USE_RUN_LEN && freq >= MIN_FREQUENCY_TO_USE_RUN_LEN && count < n
}

pub fn run_len_jumpstart(count: usize, n: usize) -> Option<Bitlen> {
  let freq = (count as f64) / (n as f64);
  if bin_needs_run_len(count, n, freq) {
    let non_freq = 1.0 - freq;
    Some(min(
      (-non_freq.log2()).ceil() as Bitlen,
      MAX_JUMPSTART,
    ))
  } else {
    None
  }
}

#[inline]
pub fn weight_and_jumpstart_cost(count: usize, n: usize) -> (usize, f64) {
  let freq = (count as f64) / (n as f64);
  if bin_needs_run_len(count, n, freq) {
    let non_freq = 1.0 - freq;
    let weight = (freq * non_freq * n as f64).ceil() as usize;
    let jumpstart_cost = (-non_freq.log2()).ceil() + 1.0;
    (weight, jumpstart_cost)
  } else {
    (count, 0.0)
  }
}

pub fn use_run_len<U: UnsignedLike>(bins: &[Bin<U>]) -> bool {
  bins.iter().any(|p| p.run_len_jumpstart.is_some())
}

pub trait RunLenOperator {
  // returns count of numbers processed
  fn unchecked_decompress_for_bin<U: UnsignedLike, M: Mode<U>>(
    state: &mut num_decompressor::State<M::Bin>,
    reader: &mut BitReader,
    bin: &BinDecompressionInfo<M::Bin>,
    mode: M,
    dest: &mut [U],
  ) -> usize;

  fn batch_ongoing(len: usize, batch_size: usize) -> bool;
}

pub struct GeneralRunLenOp;

impl RunLenOperator for GeneralRunLenOp {
  #[inline]
  fn unchecked_decompress_for_bin<U: UnsignedLike, M: Mode<U>>(
    state: &mut num_decompressor::State<M::Bin>,
    reader: &mut BitReader,
    bin: &BinDecompressionInfo<M::Bin>,
    mode: M,
    dest: &mut [U],
  ) -> usize {
    match bin.run_len_jumpstart {
      None => {
        dest[0] = mode.unchecked_decompress_unsigned(&bin.mode_bin, reader);
        1
      }
      // we stored the number of occurrences minus 1 because we knew it's at least 1
      Some(jumpstart) => {
        let full_reps = reader.unchecked_read_varint(jumpstart) + 1;
        let reps = state.unchecked_limit_reps(bin.mode_bin, full_reps, dest.len());
        for i in 0..reps {
          dest[i] = mode.unchecked_decompress_unsigned(&bin.mode_bin, reader);
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
    _state: &mut num_decompressor::State<M::Bin>,
    reader: &mut BitReader,
    bin: &BinDecompressionInfo<M::Bin>,
    mode: M,
    dest: &mut [U],
  ) -> usize {
    dest[0] = mode.unchecked_decompress_unsigned(&bin.mode_bin, reader);
    1
  }

  #[inline]
  fn batch_ongoing(_len: usize, _batch_size: usize) -> bool {
    true
  }
}
