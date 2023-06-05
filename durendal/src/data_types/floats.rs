use std::cmp::{max, min, Ordering};

use crate::constants::Bitlen;
use crate::data_types::{FloatLike, NumberLike};

// Note that in all conversions between float and unsigned int, we are using
// the unsigned int to indicate an offset.
// For instance, since f32 has 23 fraction bits, here we want 1.0 + 3_u32 to be
// 1.0 + (3.0 * 2.0 ^ -23).
macro_rules! impl_float_number {
  ($t: ty, $signed: ty, $unsigned: ty, $bits: expr, $sign_bit_mask: expr, $header_byte: expr) => {
    impl FloatLike for $t {
      const PRECISION_BITS: Bitlen = Self::MANTISSA_DIGITS - 1;
      const GREATEST_PRECISE_INT: Self = (1_u64 << Self::MANTISSA_DIGITS) as Self;

      fn abs(self) -> Self {
        self.abs()
      }

      fn inv(self) -> Self {
        1.0 / self
      }

      fn round(self) -> Self {
        self.round()
      }

      fn from_u64_numerical(x: u64) -> Self {
        x as Self
      }

      fn log2_epsilons_between_positives(a: Self, b: Self) -> Bitlen {
        let (a, b) = (a.to_bits(), b.to_bits());
        let epsilons = max(a, b) - min(a, b);
        $bits - epsilons.leading_zeros()
      }

      fn total_cmp(a: &Self, b: &Self) -> Ordering {
        Self::total_cmp(a, b)
      }
    }

    impl NumberLike for $t {
      const HEADER_BYTE: u8 = $header_byte;
      const PHYSICAL_BITS: usize = $bits;
      const IS_FLOAT: bool = true;

      type Unsigned = $unsigned;

      fn assert_float(nums: &[Self]) -> &[Self] {
        nums
      }

      #[inline]
      fn to_unsigned(self) -> Self::Unsigned {
        let mem_layout = self.to_bits();
        if mem_layout & $sign_bit_mask > 0 {
          // negative float
          !mem_layout
        } else {
          // positive float
          mem_layout ^ $sign_bit_mask
        }
      }

      #[inline]
      fn from_unsigned(off: Self::Unsigned) -> Self {
        if off & $sign_bit_mask > 0 {
          // positive float
          Self::from_bits(off ^ $sign_bit_mask)
        } else {
          // negative float
          Self::from_bits(!off)
        }
      }

      #[inline]
      fn transmute_to_unsigned_slice(slice: &mut [Self]) -> &mut [Self::Unsigned] {
        unsafe { std::mem::transmute(slice) }
      }

      #[inline]
      fn transmute_to_unsigned(self) -> Self::Unsigned {
        self.to_bits()
      }
    }
  };
}

impl_float_number!(f32, i32, u32, 32, 1_u32 << 31, 6);
impl_float_number!(f64, i64, u64, 64, 1_u64 << 63, 5);
