use std::cmp::min;
use crate::bit_reader::BitReader;
use crate::constants::{MAX_JUMPSTART, MIN_FREQUENCY_TO_USE_RUN_LEN, MIN_N_TO_USE_RUN_LEN};
use crate::data_types::{NumberLike, UnsignedLike};
use crate::gcd_utils::GcdOperator;
use crate::num_decompressor::NumDecompressor;
use crate::prefix::PrefixDecompressionInfo;
use crate::Prefix;

fn prefix_needs_run_len(count: usize, n: usize, freq: f64) -> bool {
  n >= MIN_N_TO_USE_RUN_LEN
    && freq >= MIN_FREQUENCY_TO_USE_RUN_LEN
    && count < n
}

pub fn run_len_jumpstart(count: usize, n: usize) -> Option<usize> {
  let freq = (count as f64) / (n as f64);
  if prefix_needs_run_len(count, n, freq) {
    let non_freq = 1.0 - freq;
    Some(min(
      (-non_freq.log2()).ceil() as usize,
      MAX_JUMPSTART,
    ))
  } else {
    None
  }
}

#[inline]
pub fn weight_and_jumpstart_cost(count: usize, n: usize) -> (usize, f64) {
  let freq = (count as f64) / (n as f64);
  if prefix_needs_run_len(count, n, freq) {
    let non_freq = 1.0 - freq;
    let weight = (freq * non_freq * n as f64).ceil() as usize;
    let jumpstart_cost = (-non_freq.log2()).ceil() + 1.0;
    (weight, jumpstart_cost)
  } else {
    (count, 0.0)
  }
}

pub fn use_run_len<T: NumberLike>(prefixes: &[Prefix<T>]) -> bool {
  prefixes.iter().any(|p| p.run_len_jumpstart.is_some())
}

fn unchecked_decompress_offset<U: UnsignedLike, GcdOp: GcdOperator<U>>(
  reader: &mut BitReader,
  unsigneds: &mut Vec<U>,
  p: PrefixDecompressionInfo<U>,
) {
  let mut offset = reader.unchecked_read_uint(p.k);
  if offset < p.min_unambiguous_k_bit_offset && reader.unchecked_read_one() {
    offset |= p.most_significant;
  }
  let unsigned = p.lower_unsigned + GcdOp::get_diff(offset, p.gcd);
  unsigneds.push(unsigned);
}

pub trait RunLenOperator {
  fn unchecked_decompress_offsets<U: UnsignedLike, GcdOp: GcdOperator<U>>(
    num_decompressor: &mut NumDecompressor<U>,
    reader: &mut BitReader,
    unsigneds: &mut Vec<U>,
    p: PrefixDecompressionInfo<U>,
    batch_size: usize,
  );

  fn batch_ongoing(len: usize, batch_size: usize) -> bool;
}

pub struct GeneralRunLenOp;

impl RunLenOperator for GeneralRunLenOp {
  fn unchecked_decompress_offsets<U: UnsignedLike, GcdOp: GcdOperator<U>>(
    num_decompressor: &mut NumDecompressor<U>,
    reader: &mut BitReader,
    unsigneds: &mut Vec<U>,
    p: PrefixDecompressionInfo<U>,
    batch_size: usize,
  ) {
    match p.run_len_jumpstart {
      None => unchecked_decompress_offset::<U, GcdOp>(reader, unsigneds, p),
      // we stored the number of occurrences minus 1 because we knew it's at least 1
      Some(jumpstart) => {
        let full_reps = reader.unchecked_read_varint(jumpstart) + 1;
        let reps =
          num_decompressor.unchecked_limit_reps(p, full_reps, batch_size - unsigneds.len());
        if p.k == 0 {
          for _ in 0..reps {
            unsigneds.push(p.lower_unsigned);
          }
        } else {
          for _ in 0..reps {
            unchecked_decompress_offset::<U, GcdOp>(reader, unsigneds, p);
          }
        }
      }
    };
  }

  #[inline]
  fn batch_ongoing(len: usize, batch_size: usize) -> bool {
    len < batch_size
  }
}

pub struct TrivialRunLenOp;

impl RunLenOperator for TrivialRunLenOp {
  fn unchecked_decompress_offsets<U: UnsignedLike, GcdOp: GcdOperator<U>>(
    _num_decompressor: &mut NumDecompressor<U>,
    reader: &mut BitReader,
    unsigneds: &mut Vec<U>,
    p: PrefixDecompressionInfo<U>,
    _batch_size: usize,
  ) {
    unchecked_decompress_offset::<U, GcdOp>(reader, unsigneds, p)
  }

  #[inline]
  fn batch_ongoing(_len: usize, _batch_size: usize) -> bool {
    true
  }
}
