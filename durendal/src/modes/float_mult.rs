use std::cmp::{max, min};
use crate::bin::BinCompressionInfo;
use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::constants::{Bitlen, BITS_TO_ENCODE_ADJ_BITS, UNSIGNED_BATCH_SIZE};
use crate::{Bin, bits};
use std::marker::PhantomData;

use crate::data_types::{FloatLike, NumberLike, UnsignedLike};
use crate::errors::QCompressResult;
use crate::modes::{Mode, ModeBin};

// We'll only consider using FloatMultMode if we can save at least 1/this of the
// mantissa bits by using it.
const REQUIRED_INFORMATION_GAIN_DENOM: Bitlen = 6;

pub fn adj_bits_needed<U: UnsignedLike>(base: U::Float, inv_base: U::Float, sorted: &[U]) -> Bitlen {
  let mut max_adj_bits = 0;
  for &u in sorted {
    let x = U::Float::from_unsigned(u);
    let approx = ((x * inv_base).round() * base).to_unsigned();
    let adj_bits = bits::bits_to_encode_offset((max(u, approx) - min(u, approx)) << 1);
    max_adj_bits = max(max_adj_bits, adj_bits);
  }
  max_adj_bits
}

pub fn calc_adj_lower<U: UnsignedLike>(adj_offset_bits: Bitlen) -> U {
  if adj_offset_bits == 0 {
    U::ZERO
  } else {
    U::ZERO.wrapping_sub(U::ONE << (adj_offset_bits - 1))
  }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct FloatMultBin<U: UnsignedLike> {
  mult_lower: U::Float,
  mult_offset_bits: Bitlen,
  adj_lower: U,
  adj_offset_bits: Bitlen,
}

impl<U: UnsignedLike> ModeBin for FloatMultBin<U> {}

#[derive(Clone, Copy, Debug)]
pub struct FloatMultMode<U: UnsignedLike> {
  base: U::Float,
  inv_base: U::Float,
}

impl<U: UnsignedLike> FloatMultMode<U> {
  pub fn new(inv_base: U::Float) -> Self {
    Self {
      base: inv_base.inv(),
      inv_base,
    }
  }

  fn calc_offset_bits(&self, lower: U, upper: U) -> Bitlen {
    let delta = self.inv_base * U::Float::from_unsigned(upper) - self.inv_base * U::Float::from_unsigned(lower);
    U::BITS - delta.round().to_unsigned().leading_zeros()
  }
}

impl<U: UnsignedLike> Mode<U> for FloatMultMode<U> {
  const EXTRA_META_COST: f64 = BITS_TO_ENCODE_ADJ_BITS as f64;

  type BinOptAccumulator = Bitlen;
  fn combine_bin_opt_acc(bin: &BinCompressionInfo<U>, acc: &mut Self::BinOptAccumulator) {
    *acc = max(*acc, bin.adj_bits);
  } // adj bits

  fn bin_cost(&self, lower: U, upper: U, count: usize, acc: &Self::BinOptAccumulator) -> f64 {
    let offset_bits = self.calc_offset_bits(lower, upper);
    (count * (acc + offset_bits) as usize) as f64
  }

  fn fill_optimized_compression_info(&self, acc: Self::BinOptAccumulator, bin: &mut BinCompressionInfo<U>) {
    bin.offset_bits = self.calc_offset_bits(bin.lower, bin.upper);
    bin.adj_bits = acc;
    bin.adj_lower = calc_adj_lower(acc);
  }

  #[inline]
  fn compress_offset(&self, u: U, bin: &BinCompressionInfo<U>, writer: &mut BitWriter) {
    let float = U::Float::from_unsigned(u);
    let mult_offset = (float * self.inv_base - bin.float_mult_lower).round();
    writer.write_diff(
      U::from_float_numerical(mult_offset),
      bin.offset_bits,
    );
    let mult = mult_offset + bin.float_mult_lower;
    let approx = mult * self.base;
    let adj = u.wrapping_sub(approx.to_unsigned());
    writer.write_diff(adj.wrapping_sub(bin.adj_lower), bin.adj_bits);
  }

  type Bin = FloatMultBin<U>;

  fn make_mode_bin(bin: &Bin<U>) -> FloatMultBin<U> {
    FloatMultBin {
      mult_lower: bin.float_mult_base,
      mult_offset_bits: bin.offset_bits,
      adj_lower: calc_adj_lower(bin.adj_bits),
      adj_offset_bits: bin.adj_bits,
    }
  }

  #[inline]
  fn unchecked_decompress_unsigned(&self, bin: &FloatMultBin<U>, reader: &mut BitReader) -> U {
    let offset = reader.unchecked_read_uint::<U>(bin.mult_offset_bits);
    let mult = bin.mult_lower + U::to_float_numerical(offset);
    let approx = mult * self.base;
    let adj = bin
      .adj_lower
      .wrapping_add(reader.unchecked_read_uint(bin.adj_offset_bits));
    approx.to_unsigned().wrapping_add(adj)
  }

  #[inline]
  fn decompress_unsigned(
    &self,
    bin: &FloatMultBin<U>,
    reader: &mut BitReader,
  ) -> QCompressResult<U> {
    let offset = reader.read_uint::<U>(bin.mult_offset_bits)?;
    let mult = bin.mult_lower + U::to_float_numerical(offset);
    let approx = mult * self.base;
    let adj = bin
      .adj_lower
      .wrapping_add(reader.read_uint(bin.adj_offset_bits)?);
    Ok(approx.to_unsigned().wrapping_add(adj))
  }
}

enum StrategyChainResult {
  CloseToExactMultiple,
  FarFromExactMultiple,
  Uninformative, // the base is not much bigger than machine epsilon
}

struct StrategyChain<U: UnsignedLike> {
  bases_and_invs: Vec<(U::Float, U::Float)>,
  candidate_idx: Option<usize>,
  pub proven_useful: bool,
  phantom: PhantomData<U>,
}

impl<U: UnsignedLike> StrategyChain<U> {
  fn inv_powers_of(inv_base_0: u64, n_powers: u32) -> Self {
    let mut inv_base = inv_base_0;
    let mut bases_and_invs = Vec::new();
    for _ in 0..n_powers {
      let inv_base_float = U::Float::from_u64_numerical(inv_base);
      bases_and_invs.push((inv_base_float.inv(), inv_base_float));
      inv_base *= inv_base_0;
    }

    Self {
      bases_and_invs,
      candidate_idx: Some(0),
      proven_useful: false,
      phantom: PhantomData,
    }
  }

  fn current_base_and_inv(&self) -> Option<(U::Float, U::Float)> {
    self
      .candidate_idx
      .and_then(|idx| self.bases_and_invs.get(idx).cloned())
  }

  fn current_inv_base(&self) -> Option<U::Float> {
    self.current_base_and_inv().map(|(_, inv_base)| inv_base)
  }

  fn compatibility_with(&self, sorted_chunk: &[U]) -> StrategyChainResult {
    match self.current_base_and_inv() {
      Some((base, inv_base)) => {
        let mut res = StrategyChainResult::Uninformative;
        let mut seen_mult: Option<U::Float> = None;
        let required_information_gain = U::Float::PRECISION_BITS / REQUIRED_INFORMATION_GAIN_DENOM;

        for &u in sorted_chunk {
          let abs_float = U::Float::from_unsigned(u).abs();
          let base_bits = U::Float::log2_epsilons_between_positives(abs_float, abs_float + base);
          let mult = (abs_float * inv_base).round();
          let adj_bits = U::Float::log2_epsilons_between_positives(abs_float, mult * base);

          if adj_bits > base_bits.saturating_sub(required_information_gain) {
            return StrategyChainResult::FarFromExactMultiple;
          } else if base_bits >= required_information_gain {
            match seen_mult {
              Some(a_mult) if mult != a_mult => {
                res = StrategyChainResult::CloseToExactMultiple;
              }
              _ => {
                seen_mult = Some(mult)
              }
            }
          }
        }

        res
      }
      None => StrategyChainResult::Uninformative,
    }
  }

  fn is_invalid(&self) -> bool {
    self.current_base_and_inv().is_none()
  }

  pub fn relax(&mut self) {
    self.candidate_idx.iter_mut().for_each(|idx| *idx += 1);
  }

  fn invalidate(&mut self) {
    self.candidate_idx = None;
  }
}

// We'll go through all the nums and check if each one is numerically close to
// a multiple of the first base in each chain. If not, we'll fall back to the
// 2nd base here, and so forth, assuming that all numbers divisible by the nth
// base are also divisible by the n+1st.
pub struct Strategy<U: UnsignedLike> {
  chains: Vec<StrategyChain<U>>,
}

impl<U: UnsignedLike> Strategy<U> {
  pub fn choose_base_and_inv(&mut self, sorted: &[U]) -> Option<(U::Float, U::Float)> {
    let smallest = U::Float::from_unsigned(sorted[0]);
    let biggest = U::Float::from_unsigned(*sorted.last().unwrap());
    let biggest_float = [smallest, biggest, biggest - smallest]
      .iter()
      .map(|x| x.abs())
      .max_by(U::Float::total_cmp)
      .unwrap();

    let mut invalid_count = 0;
    for chunk in sorted.chunks(UNSIGNED_BATCH_SIZE) {
      if invalid_count == self.chains.len() {
        break;
      }

      for chain in &mut self.chains {
        if chain.is_invalid() {
          continue;
        }

        // but NANs are ok
        if biggest_float * chain.current_inv_base().unwrap() >= U::Float::GREATEST_PRECISE_INT {
          invalid_count += 1;
          chain.invalidate();
        }

        loop {
          let compatibility = chain.compatibility_with(chunk);
          match compatibility {
            StrategyChainResult::FarFromExactMultiple => chain.relax(),
            StrategyChainResult::CloseToExactMultiple => {
              chain.proven_useful = true;
              break;
            }
            _ => break,
          }
        }
      }
    }

    self
      .chains
      .iter()
      .flat_map(|chain| {
        if chain.proven_useful {
          chain.current_inv_base()
        } else {
          None
        }
      })
      .max_by(U::Float::total_cmp)
      .map(|inv_base| (inv_base.inv(), inv_base))
  }
}

impl<U: UnsignedLike> Default for Strategy<U> {
  fn default() -> Self {
    // 0.1, 0.01, ... 10^-9
    Self {
      chains: vec![StrategyChain::inv_powers_of(10, 9)],
    }
  }
}

#[cfg(test)]
mod test {
  use super::*;
  use crate::bit_words::BitWords;
  use crate::constants::Bitlen;
  use crate::data_types::NumberLike;

  fn make_bin(
    float_mult_lower: f64,
    offset_bits: Bitlen,
    adj_bits: Bitlen,
  ) -> BinCompressionInfo<u64> {
    BinCompressionInfo {
      count: 0,
      code: 0,
      code_len: 0,
      run_len_jumpstart: None,
      lower: 0,
      upper: 0,
      gcd: 1,
      offset_bits,
      float_mult_lower,
      adj_lower: calc_adj_lower(adj_bits),
      adj_bits,
    }
  }

  fn check(
    mode: FloatMultMode<u64>,
    c_info: BinCompressionInfo<u64>,
    x: f64,
    desc: &str,
  ) -> QCompressResult<()> {
    let bin = Bin::from(c_info);
    let d_info = FloatMultMode::<u64>::make_mode_bin(&bin);
    let u = x.to_unsigned();
    let mut writer = BitWriter::default();
    mode.compress_offset(u, &c_info, &mut writer);
    let words = BitWords::from(writer.drain_bytes());
    let mut reader0 = BitReader::from(&words);
    let mut reader1 = BitReader::from(&words);
    let recovered = mode.unchecked_decompress_unsigned(&d_info, &mut reader0);
    let recovered_float = f64::from_unsigned(recovered);
    assert_eq!(
      recovered, u,
      "{} unchecked: {} vs {}",
      desc, recovered_float, x
    );
    let recovered = mode.decompress_unsigned(&d_info, &mut reader1)?;
    assert_eq!(
      recovered, u,
      "{} checked: {} vs {}",
      desc, recovered_float, x
    );
    Ok(())
  }

  #[test]
  fn test_float_mult_lossless() -> QCompressResult<()> {
    let mode = FloatMultMode::<u64>::new(10.0);
    // bin with exact arithmetic
    let bin = make_bin(5.0, 0, 0);
    check(mode, bin, 0.5, "empty bin exact")?;

    // 0.1 * 3.0 overshoots by exactly 1 machine epsilon
    let bin = make_bin(3.0, 0, 1);
    check(mode, bin, 0.3, "inexact bin")?;

    // ~[-1.0, 2.1]
    let bin = make_bin(-10.0, 5, 3);
    check(mode, bin, -1.0, "regular -1.0")?;
    check(mode, bin, -1.0 + 0.1, "regular -0.9")?;
    check(mode, bin, -0.0, "regular -0")?;
    check(mode, bin, 0.0, "regular 0")?;
    check(mode, bin, 2.1, "regular 2.1")?;

    // edge cases
    let bin = make_bin(f64::NAN, 0, 0);
    check(mode, bin, f64::NAN, "nan")?;
    let bin = make_bin(f64::NEG_INFINITY, 0, 0);
    check(mode, bin, f64::NEG_INFINITY, "nan")?;

    Ok(())
  }

  #[test]
  fn test_choose_base() {
    fn inv_base(floats: Vec<f64>) -> Option<f64> {
      let mut strategy = Strategy::<u64>::default();
      let sorted = floats.iter().map(|x| x.to_unsigned()).collect::<Vec<_>>();
      strategy
        .choose_base_and_inv(&sorted)
        .map(|(_, inv_base)| inv_base)
    }

    let floats = vec![-0.1, 0.1, 0.100000000001, 0.33, 1.01, 1.1];
    assert_eq!(inv_base(floats), Some(100.0));

    let floats = vec![
      -f64::NEG_INFINITY,
      -f64::NAN,
      -0.1,
      1.0,
      1.1,
      f64::NAN,
      f64::INFINITY,
    ];
    assert_eq!(inv_base(floats), Some(10.0));

    let floats = vec![-(2.0_f64.powi(53)), -0.1, 1.0, 1.1];
    assert_eq!(inv_base(floats), None);

    let floats = vec![-0.1, 1.0, 1.1, 2.0_f64.powi(53)];
    assert_eq!(inv_base(floats), None);

    let floats = vec![1.0 / 7.0, 2.0 / 7.0];
    assert_eq!(inv_base(floats), None);

    let floats = vec![1.0, 1.00000000000001, 0.99999999999999];
    assert_eq!(inv_base(floats), None);
  }
}
