use crate::bin::BinDecompressionInfo;
use crate::bit_reader::BitReader;
use crate::data_types::{NumberLike, UnsignedLike};
use crate::gcd_utils::GcdOperator;
use crate::num_decompressor::NumDecompressor;
use crate::Bin;

pub fn use_run_len<T: NumberLike>(bins: &[Bin<T>]) -> bool {
  bins.iter().any(|p| p.run_len_jumpstart.is_some())
}

fn unchecked_decompress_offset<U: UnsignedLike, GcdOp: GcdOperator<U>>(
  reader: &mut BitReader,
  unsigneds: &mut Vec<U>,
  p: BinDecompressionInfo<U>,
) {
  let offset = reader.unchecked_read_uint(p.offset_bits);
  let unsigned = p.lower_unsigned + GcdOp::get_diff(offset, p.gcd);
  unsigneds.push(unsigned);
}

pub trait RunLenOperator {
  fn unchecked_decompress_offsets<U: UnsignedLike, GcdOp: GcdOperator<U>>(
    num_decompressor: &mut NumDecompressor<U>,
    reader: &mut BitReader,
    unsigneds: &mut Vec<U>,
    p: BinDecompressionInfo<U>,
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
    p: BinDecompressionInfo<U>,
    batch_size: usize,
  ) {
    match p.run_len_jumpstart {
      None => unchecked_decompress_offset::<U, GcdOp>(reader, unsigneds, p),
      // we stored the number of occurrences minus 1 because we knew it's at least 1
      Some(jumpstart) => {
        let full_reps = reader.unchecked_read_varint(jumpstart) + 1;
        let reps =
          num_decompressor.unchecked_limit_reps(p, full_reps, batch_size - unsigneds.len());
        if p.offset_bits == 0 {
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
    p: BinDecompressionInfo<U>,
    _batch_size: usize,
  ) {
    unchecked_decompress_offset::<U, GcdOp>(reader, unsigneds, p)
  }

  #[inline]
  fn batch_ongoing(_len: usize, _batch_size: usize) -> bool {
    true
  }
}
