use crate::bin::BinCompressionInfo;
use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::constants::Bitlen;
use crate::Bin;

use crate::data_types::{FloatLike, NumberLike, UnsignedLike};
use crate::errors::QCompressResult;
use crate::modes::{Mode, ModeBin};

fn calc_adj_lower<U: UnsignedLike>(adj_offset_bits: Bitlen) -> U {
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
  pub fn new(base: U::Float) -> Self {
    Self {
      base,
      inv_base: base.inv(),
    }
  }
}

impl<U: UnsignedLike> Mode<U> for FloatMultMode<U> {
  type Bin = FloatMultBin<U>;

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
    println!("C mult_base {} mult {} approx {} adj {}", bin.float_mult_lower, mult, approx, adj);
    writer.write_diff(adj.wrapping_sub(bin.adj_lower), bin.adj_bits);
  }

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
    let adj = bin.adj_lower.wrapping_add(reader.unchecked_read_uint(bin.adj_offset_bits));
    println!("DU offset {} mult_base {} mult {} approx {} adj {}", offset, bin.mult_lower, mult, approx, adj);
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
    let adj = bin.adj_lower.wrapping_add(reader.read_uint(bin.adj_offset_bits)?);
    println!("DU offset {} mult_base {} mult {} approx {} adj {}", offset, bin.mult_lower, mult, approx, adj);
    Ok(approx.to_unsigned().wrapping_add(adj))
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
    assert_eq!(
      f64::from_unsigned(recovered),
      x,
      "{} unchecked float",
      desc
    );
    assert_eq!(recovered, u, "{} unchecked", desc);
    let recovered = mode.decompress_unsigned(&d_info, &mut reader1)?;
    assert_eq!(recovered, u, "{} checked", desc);
    Ok(())
  }

  #[test]
  fn test_float_mult_lossless() -> QCompressResult<()> {
    let mode = FloatMultMode::<u64>::new(0.1);
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
}
