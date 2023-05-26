use crate::data_types::{UnsignedLike};

use crate::base_compressor::InternalCompressorConfig;
use crate::bin::{BinCompressionInfo, BinDecompressionInfo};
use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::errors::QCompressResult;
use crate::modes::Mode;
use crate::Bin;


// formula: bin lower + offset * bin gcd
#[derive(Clone, Copy, Debug)]
pub struct GcdMode;

impl<U: UnsignedLike> Mode<U> for GcdMode {
  #[inline]
  fn compress_offset(&self, u: U, bin: &BinCompressionInfo<U>, writer: &mut BitWriter) {
    writer.write_diff((u - bin.lower) / bin.gcd, bin.offset_bits);
  }

    fn make_decompression_info(bin: &Bin<U>) -> BinDecompressionInfo<U> {
    BinDecompressionInfo {
      depth: bin.code_len,
      run_len_jumpstart: bin.run_len_jumpstart,
      unsigned0: bin.lower,
      unsigned1: bin.gcd,
      bitlen0: bin.offset_bits,
      bitlen1: 0,
    }
  }

  #[inline]
  fn unchecked_decompress_unsigned(
    &self,
    bin: &BinDecompressionInfo<U>,
    reader: &mut BitReader,
  ) -> U {
    bin.unsigned0 + reader.unchecked_read_uint::<U>(bin.bitlen0) * bin.unsigned1
  }

  #[inline]
  fn decompress_unsigned(
    &self,
    bin: &BinDecompressionInfo<U>,
    reader: &mut BitReader,
  ) -> QCompressResult<U> {
    Ok(bin.unsigned0 + reader.read_uint::<U>(bin.bitlen0)? * bin.unsigned1)
  }
}

// fast if b is small, requires b > 0
pub fn pair_gcd<U: UnsignedLike>(mut a: U, mut b: U) -> U {
  loop {
    a %= b;
    if a == U::ZERO {
      return b;
    }
    b %= a;
    if b == U::ZERO {
      return a;
    }
  }
}

pub fn gcd<U: UnsignedLike>(sorted: &[U]) -> U {
  let lower = sorted[0];
  let upper = sorted[sorted.len() - 1];
  if lower == upper {
    return U::ONE;
  }
  let mut res = upper - lower;
  for &x in sorted.iter().skip(1) {
    if res == U::ONE {
      break;
    }
    res = pair_gcd(x - lower, res);
  }
  res
}

// Returns Some(gcd) if it is more concise to use the same GCD for all bins
// Returns None if it is more concise to describe each bin's GCD separately
// 4 cases:
// * no bins: we don't even need to bother writing a common GCD, return None
// * all bins have range 0, i.e. [x, x]: GCD doesn't affect num blocks, return Some(1)
// * all bins with range >0 have same GCD: return Some(that GCD)
// * two bins with range >0 have different GCD: return None
pub fn common_gcd_for_chunk_meta<U: UnsignedLike>(bins: &[Bin<U>]) -> Option<U> {
  let mut nontrivial_ranges_share_gcd: bool = true;
  let mut gcd = None;
  for bin in bins {
    if bin.offset_bits > 0 {
      if gcd.is_none() {
        gcd = Some(bin.gcd);
      } else if gcd != Some(bin.gcd) {
        nontrivial_ranges_share_gcd = false;
      }
    }
  }

  match (bins.len(), nontrivial_ranges_share_gcd, gcd) {
    (0, _, _) => None,
    (_, false, _) => None,
    (_, true, Some(gcd)) => Some(gcd),
    (_, _, None) => Some(U::ONE),
  }
}

pub fn use_gcd_bin_optimize<U: UnsignedLike>(
  bins: &[BinCompressionInfo<U>],
  internal_config: &InternalCompressorConfig,
) -> bool {
  if !internal_config.use_gcds {
    return false;
  }

  for p in bins {
    if p.gcd > U::ONE {
      return true;
    }
  }
  for (i, pi) in bins.iter().enumerate().skip(1) {
    let pj = &bins[i - 1];
    if pi.offset_bits == 0 && pj.offset_bits == 0 && pj.lower + U::ONE < pi.lower {
      return true;
    }
  }
  false
}

pub fn use_gcd_arithmetic<U: UnsignedLike>(bins: &[Bin<U>]) -> bool {
  bins
    .iter()
    .any(|p| p.gcd > U::ONE && p.offset_bits > 0)
}

pub fn fold_bin_gcds_left<U: UnsignedLike>(
  left_lower: U,
  left_upper: U,
  left_gcd: U,
  right_upper: U,
  acc: &mut Option<U>,
) {
  // folding GCD's involves GCD'ing with their modulo offset and (if the new
  // range is nontrivial) with the new bin's GCD
  if left_upper != right_upper {
    *acc = Some(match *acc {
      Some(gcd) => pair_gcd(right_upper - left_upper, gcd),
      None => right_upper - left_upper,
    });
  }
  if left_upper != left_lower {
    *acc = Some(match *acc {
      Some(gcd) => pair_gcd(left_gcd, gcd),
      None => left_gcd,
    });
  }
}

#[cfg(test)]
mod tests {
  use crate::modes::gcd::*;

  #[test]
  fn test_pair_gcd() {
    assert_eq!(pair_gcd(0_u32, 14), 14);
    assert_eq!(pair_gcd(7_u32, 14), 7);
    assert_eq!(pair_gcd(8_u32, 14), 2);
    assert_eq!(pair_gcd(9_u32, 14), 1);
    assert_eq!(pair_gcd(8_u32, 20), 4);
    assert_eq!(pair_gcd(1_u32, 6), 1);
    assert_eq!(pair_gcd(6_u32, 1), 1);
    assert_eq!(pair_gcd(7, u64::MAX), 1);
    assert_eq!(pair_gcd(7, (1_u64 << 63) - 1), 7);
  }

  #[test]
  fn test_gcd() {
    assert_eq!(gcd(&[0_u32, 4, 6, 8, 10]), 2);
    assert_eq!(gcd(&[0_u32, 4, 6, 8, 10, 11]), 1);
  }
}