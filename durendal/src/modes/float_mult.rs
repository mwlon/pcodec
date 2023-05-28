use crate::bin::BinCompressionInfo;
use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::constants::Bitlen;
use crate::Bin;

use crate::data_types::{FloatLike, NumberLike, UnsignedLike};
use crate::errors::QCompressResult;
use crate::modes::{Mode, ModeBin};

#[derive(Clone, Copy, Debug, Default)]
pub struct FloatMultBin<U: UnsignedLike> {
  mult_lower: U::Float,
  mult_offset_bits: Bitlen,
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
    let mult = (float * self.inv_base).round();
    writer.write_diff(
      U::from_float_numerical(mult - bin.float_mult_base),
      bin.offset_bits,
    );
    let approx = mult * self.base;
    let adj = u.wrapping_sub(approx.to_unsigned());
    writer.write_diff(adj, bin.adj_bits);
  }

  fn make_mode_bin(bin: &Bin<U>) -> FloatMultBin<U> {
    FloatMultBin {
      mult_lower: bin.float_mult_base,
      mult_offset_bits: bin.offset_bits,
      adj_offset_bits: bin.adj_bits,
    }
  }

  #[inline]
  fn unchecked_decompress_unsigned(&self, bin: &FloatMultBin<U>, reader: &mut BitReader) -> U {
    let offset = reader.unchecked_read_uint::<U>(bin.mult_offset_bits);
    let mult = bin.mult_lower + U::to_float_numerical(offset);
    let approx = mult * self.base;
    let adj = reader.unchecked_read_uint(bin.adj_offset_bits);
    // println!("DU offset {} mult_base {} mult {} approx {} adj {}", offset, bin.unsigned0, mult, approx, adj);
    U::to_float_bits(U::from_float_bits(approx).wrapping_add(adj)).to_unsigned()
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
    let adj = reader.read_uint(bin.adj_offset_bits)?;
    // println!("DU offset {} mult_base {} mult {} approx {} adj {}", offset, bin.unsigned0, mult, approx, adj);
    Ok(U::to_float_bits(U::from_float_bits(approx).wrapping_add(adj)).to_unsigned())
  }
}

#[cfg(test)]
mod test {
  use super::*;
  use crate::bit_words::BitWords;
  use crate::constants::Bitlen;
  use crate::data_types::NumberLike;

  fn make_bin(
    offset_bits: Bitlen,
    float_mult_base: f64,
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
      float_mult_base,
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
    let empty_bin_exact = make_bin(0, 5.0, 0);
    check(mode, empty_bin_exact, 0.5, "empty bin exact")?;

    // 0.1 * 3 overshoots by exactly 1 machine epsilon
    let empty_bin_inexact = make_bin(0, 3.0, 1);
    check(mode, empty_bin_inexact, 0.3, "inexact bin")?;

    // ~[-1.0, 2.1]
    let regular_bin = make_bin(5, -10.0, 2);
    check(mode, regular_bin, -1.0, "regular -1.0")?;
    check(mode, regular_bin, -1.0 + 0.1, "regular -0.9")?;
    check(mode, regular_bin, -0.0, "regular -0")?;
    check(mode, regular_bin, 0.0, "regular 0")?;
    check(mode, regular_bin, 2.1, "regular 2.1")?;
    Ok(())
  }
}
