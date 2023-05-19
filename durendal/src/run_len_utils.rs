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
  p: BinDecompressionInfo<U>,
) -> U {
  let offset = reader.unchecked_read_uint(p.offset_bits);
  p.lower_unsigned + GcdOp::get_diff(offset, p.gcd)
}

pub trait RunLenOperator {
  // returns count of numbers processed
  fn unchecked_decompress_offsets<U: UnsignedLike, GcdOp: GcdOperator<U>>(
    num_decompressor: &mut NumDecompressor<U>,
    reader: &mut BitReader,
    p: BinDecompressionInfo<U>,
    dest: &mut [U],
  ) -> usize;

  fn batch_ongoing(len: usize, batch_size: usize) -> bool;
}

pub struct GeneralRunLenOp;

impl RunLenOperator for GeneralRunLenOp {
  fn unchecked_decompress_offsets<U: UnsignedLike, GcdOp: GcdOperator<U>>(
    num_decompressor: &mut NumDecompressor<U>,
    reader: &mut BitReader,
    p: BinDecompressionInfo<U>,
    dest: &mut [U],
  ) -> usize {
    match p.run_len_jumpstart {
      None => {
        dest[0] = unchecked_decompress_offset::<U, GcdOp>(reader, p);
        1
      },
      // we stored the number of occurrences minus 1 because we knew it's at least 1
      Some(jumpstart) => {
        let full_reps = reader.unchecked_read_varint(jumpstart) + 1;
        let reps = num_decompressor.unchecked_limit_reps(p, full_reps, dest.len());
        if p.offset_bits == 0 {
          for i in 0..reps {
            dest[i] = p.lower_unsigned;
          }
        } else {
          for i in 0..reps {
            dest[i] = unchecked_decompress_offset::<U, GcdOp>(reader, p);
          }
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
  fn unchecked_decompress_offsets<U: UnsignedLike, GcdOp: GcdOperator<U>>(
    _num_decompressor: &mut NumDecompressor<U>,
    reader: &mut BitReader,
    p: BinDecompressionInfo<U>,
    dest: &mut [U],
  ) -> usize {
    dest[0] = unchecked_decompress_offset::<U, GcdOp>(reader, p);
    1
  }

  #[inline]
  fn batch_ongoing(_len: usize, _batch_size: usize) -> bool {
    true
  }
}
