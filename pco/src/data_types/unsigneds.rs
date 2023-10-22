use crate::constants::Bitlen;
use crate::data_types::{FloatLike, NumberLike, UnsignedLike};

macro_rules! impl_unsigned {
  ($t: ty, $float: ty, $signed: ty) => {
    impl UnsignedLike for $t {
      const ZERO: Self = 0;
      const ONE: Self = 1;
      const MID: Self = 1 << (Self::BITS - 1);
      const MAX: Self = Self::MAX;
      const BITS: Bitlen = Self::BITS as Bitlen;

      type Float = $float;

      #[inline]
      fn from_word(word: usize) -> Self {
        word as Self
      }

      #[inline]
      fn leading_zeros(self) -> Bitlen {
        self.leading_zeros() as Bitlen
      }

      #[inline]
      fn to_usize(self) -> usize {
        self as usize
      }

      #[inline]
      fn wrapping_add(self, other: Self) -> Self {
        self.wrapping_add(other)
      }

      #[inline]
      fn wrapping_sub(self, other: Self) -> Self {
        self.wrapping_sub(other)
      }

      #[inline]
      fn to_int_float(self) -> Self::Float {
        let (negative, abs_int) = if self >= Self::MID {
          (false, self - Self::MID)
        } else {
          (true, Self::MID - 1 - self)
        };
        let gpi = <$float>::GREATEST_PRECISE_INT;
        let abs_float = if abs_int < gpi as Self {
          abs_int as $float
        } else {
          <$float>::from_bits(gpi.to_bits() + (abs_int - gpi as Self))
        };
        if negative {
          -abs_float
        } else {
          abs_float
        }
      }

      #[inline]
      fn from_int_float(float: Self::Float) -> Self {
        let abs = float.abs();
        let gpi = <$float>::GREATEST_PRECISE_INT;
        let abs_int = if abs < gpi {
          abs as Self
        } else {
          gpi as Self + (abs.to_bits() - gpi.to_bits())
        };
        if float.is_sign_positive() {
          Self::MID + abs_int
        } else {
          // -1 because we need to distinguish -0.0 from +0.0
          Self::MID - 1 - abs_int
        }
      }

      #[inline]
      fn to_float_bits(self) -> Self::Float {
        Self::Float::from_bits(self)
      }

      #[inline]
      fn from_float_bits(float: Self::Float) -> Self {
        float.to_bits()
      }
    }
  };
}

impl_unsigned!(u32, f32, i32);
impl_unsigned!(u64, f64, i64);

macro_rules! impl_unsigned_number {
  ($t: ty, $signed: ty, $float: ty, $header_byte: expr) => {
    impl NumberLike for $t {
      const HEADER_BYTE: u8 = $header_byte;
      const PHYSICAL_BITS: usize = Self::BITS as usize;

      type Unsigned = Self;

      #[inline]
      fn to_unsigned(self) -> Self::Unsigned {
        self
      }

      #[inline]
      fn from_unsigned(off: Self::Unsigned) -> Self {
        off
      }

      #[inline]
      fn transmute_to_unsigned_slice(slice: &mut [Self]) -> &mut [Self::Unsigned] {
        slice
      }

      #[inline]
      fn transmute_to_unsigned(self) -> Self::Unsigned {
        self
      }
    }
  };
}

impl_unsigned_number!(u32, i32, f32, 1);
impl_unsigned_number!(u64, i64, f64, 2);

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn int_float32_invertibility() {
    for x in [
      -f32::NAN,
      f32::NEG_INFINITY,
      f32::MIN,
      -1.0,
      -0.0,
      0.0,
      3.0,
      f32::MAX,
      f32::INFINITY,
      f32::NAN,
    ] {
      let int = u32::from_int_float(x);
      let recovered = u32::to_int_float(int);
      // gotta compare unsigneds because floats don't implement Equal
      assert_eq!(
        recovered.to_unsigned(),
        x.to_unsigned(),
        "{} != {}",
        x,
        recovered
      );
    }
  }

  #[test]
  fn int_float64_invertibility() {
    for x in [
      -f64::NAN,
      f64::NEG_INFINITY,
      f64::MIN,
      -1.0,
      -0.0,
      0.0,
      3.0,
      f64::MAX,
      f64::INFINITY,
      f64::NAN,
    ] {
      let int = u64::from_int_float(x);
      let recovered = u64::to_int_float(int);
      // gotta compare unsigneds because floats don't implement Equal
      assert_eq!(
        recovered.to_unsigned(),
        x.to_unsigned(),
        "{} != {}",
        x,
        recovered
      );
    }
  }

  #[test]
  fn int_float_ordering() {
    let values = vec![
      -f32::NAN,
      f32::NEG_INFINITY,
      f32::MIN,
      -1.0,
      -0.0,
      0.0,
      3.0,
      f32::GREATEST_PRECISE_INT,
      f32::MAX,
      f32::INFINITY,
      f32::NAN,
    ];
    let mut last_int = None;
    for x in values {
      let int = u32::from_int_float(x);
      if let Some(last_int) = last_int {
        assert!(
          last_int < int,
          "at {}; int {} vs {}",
          x,
          last_int,
          int
        );
      }
      last_int = Some(int)
    }

    assert_eq!(
      u32::from_int_float(f32::GREATEST_PRECISE_INT) - 1,
      u32::from_int_float(f32::GREATEST_PRECISE_INT - 1.0)
    );
    assert_eq!(
      u32::from_int_float(f32::GREATEST_PRECISE_INT) + 1,
      u32::from_int_float(f32::GREATEST_PRECISE_INT + 2.0)
    );
  }
}
