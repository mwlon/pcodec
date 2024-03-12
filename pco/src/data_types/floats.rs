use crate::constants::Bitlen;
use crate::data_types::{FloatLike, Latent, NumberLike, OrderedLatentConvert};

use crate::data_types::SecondaryLatents;

use crate::{float_mult_utils, ChunkConfig, FloatMultSpec, Mode};

fn choose_mode_and_split_latents<F: FloatLike>(
  nums: &[F],
  chunk_config: &ChunkConfig,
) -> (Mode<F::L>, Vec<Vec<F::L>>) {
  let classic = || {
    let primary = nums
      .iter()
      .map(|&x| x.to_latent_ordered())
      .collect::<Vec<_>>();
    (Mode::Classic, vec![primary])
  };

  match chunk_config.float_mult_spec {
    FloatMultSpec::Enabled => {
      if let Some(fm_config) = float_mult_utils::choose_config(nums) {
        let mode = Mode::float_mult(fm_config.base);
        let latents = float_mult_utils::split_latents(nums, fm_config.base, fm_config.inv_base);
        (mode, latents)
      } else {
        classic()
      }
    }
    FloatMultSpec::Provided(base_f64) => {
      let base = F::from_f64(base_f64);
      let mode = Mode::float_mult(base);
      let latents = float_mult_utils::split_latents(nums, base, base.inv());
      (mode, latents)
    }
    FloatMultSpec::Disabled => classic(),
  }
}

fn join_latents<F: FloatLike>(
  mode: Mode<F::L>,
  primary: &mut [F::L],
  secondary: SecondaryLatents<F::L>,
  dst: &mut [F],
) {
  use Mode::*;
  match mode {
    FloatMult(base_latent) => {
      let base = F::from_latent_ordered(base_latent);
      float_mult_utils::join_latents(base, primary, secondary, dst)
    }
    Classic => {
      for (&l, dst) in primary.iter().zip(dst.iter_mut()) {
        *dst = F::from_latent_ordered(l);
      }
    }
    _ => panic!("should be unreachable"),
  }
}
// Note that in all conversions between float and unsigned int, we are using
// the unsigned int to indicate an offset.
// For instance, since f32 has 23 fraction bits, here we want 1.0 + 3_u32 to be
// 1.0 + (3.0 * 2.0 ^ -23).
macro_rules! impl_float_number {
  ($t: ty, $latent: ty, $bits: expr, $sign_bit_mask: expr, $header_byte: expr, $exp_offset: expr) => {
    impl OrderedLatentConvert for $t {
      type L = $latent;

      #[inline]
      fn from_latent_ordered(l: Self::L) -> Self {
        if l & $sign_bit_mask > 0 {
          // positive float
          Self::from_bits(l ^ $sign_bit_mask)
        } else {
          // negative float
          Self::from_bits(!l)
        }
      }

      #[inline]
      fn to_latent_ordered(self) -> Self::L {
        let mem_layout = self.to_bits();
        if mem_layout & $sign_bit_mask > 0 {
          // negative float
          !mem_layout
        } else {
          // positive float
          mem_layout ^ $sign_bit_mask
        }
      }
    }

    impl FloatLike for $t {
      const BITS: Bitlen = $bits;
      const PRECISION_BITS: Bitlen = Self::MANTISSA_DIGITS as Bitlen - 1;
      const ZERO: Self = 0.0;
      const ONE: Self = 1.0;
      const MIN: Self = Self::MIN;
      const MAX: Self = Self::MAX;

      #[inline]
      fn abs(self) -> Self {
        self.abs()
      }

      fn inv(self) -> Self {
        1.0 / self
      }

      #[inline]
      fn round(self) -> Self {
        self.round()
      }

      #[inline]
      fn exp2(power: i32) -> Self {
        Self::exp2(power as Self)
      }

      #[inline]
      fn from_f64(x: f64) -> Self {
        x as Self
      }

      #[inline]
      fn to_f64(self) -> f64 {
        self as f64
      }

      #[inline]
      fn is_finite_and_normal(&self) -> bool {
        self.is_finite() && !self.is_subnormal()
      }

      #[inline]
      fn exponent(&self) -> i32 {
        (self.abs().to_bits() >> Self::PRECISION_BITS) as i32 + $exp_offset
      }

      #[inline]
      fn trailing_zeros(&self) -> u32 {
        self.to_bits().trailing_zeros()
      }

      #[inline]
      fn max(a: Self, b: Self) -> Self {
        Self::max(a, b)
      }

      #[inline]
      fn min(a: Self, b: Self) -> Self {
        Self::min(a, b)
      }

      #[inline]
      fn to_latent_bits(self) -> Self::L {
        self.to_bits()
      }

      #[inline]
      fn int_float_from_latent(l: Self::L) -> Self {
        let mid = Self::L::MID;
        let (negative, abs_int) = if l >= mid {
          (false, l - mid)
        } else {
          (true, mid - 1 - l)
        };
        let gpi = 1 << Self::MANTISSA_DIGITS;
        let abs_float = if abs_int < gpi {
          abs_int as Self
        } else {
          Self::from_bits((gpi as Self).to_bits() + (abs_int - gpi))
        };
        if negative {
          -abs_float
        } else {
          abs_float
        }
      }

      #[inline]
      fn int_float_to_latent(self) -> Self::L {
        let abs = self.abs();
        let gpi = 1 << Self::MANTISSA_DIGITS;
        let gpi_float = gpi as Self;
        let abs_int = if abs < gpi_float {
          abs as Self::L
        } else {
          gpi + (abs.to_bits() - gpi_float.to_bits())
        };
        if self.is_sign_positive() {
          Self::L::MID + abs_int
        } else {
          // -1 because we need to distinguish -0.0 from +0.0
          Self::L::MID - 1 - abs_int
        }
      }

      #[inline]
      fn from_latent_numerical(l: Self::L) -> Self {
        l as Self
      }
    }

    impl NumberLike for $t {
      const DTYPE_BYTE: u8 = $header_byte;

      type L = $latent;

      #[inline]
      fn is_identical(self, other: Self) -> bool {
        self.to_bits() == other.to_bits()
      }

      fn choose_mode_and_split_latents(
        nums: &[Self],
        config: &ChunkConfig,
      ) -> (Mode<Self::L>, Vec<Vec<Self::L>>) {
        choose_mode_and_split_latents(nums, config)
      }
      fn join_latents(
        mode: Mode<Self::L>,
        primary: &mut [Self::L],
        secondary: SecondaryLatents<Self::L>,
        dst: &mut [Self],
      ) {
        join_latents(mode, primary, secondary, dst)
      }
    }
  };
}

impl_float_number!(f32, u32, 32, 1_u32 << 31, 5, -127);
impl_float_number!(f64, u64, 64, 1_u64 << 63, 6, -1023);

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_float_ordering() {
    assert!(f32::NEG_INFINITY.to_latent_ordered() < (-0.0_f32).to_latent_ordered());
    assert!((-0.0_f32).to_latent_ordered() < (0.0_f32).to_latent_ordered());
    assert!((0.0_f32).to_latent_ordered() < f32::INFINITY.to_latent_ordered());
  }

  #[test]
  fn test_exp() {
    assert_eq!(1.0_f32.exponent(), 0);
    assert_eq!(1.0_f64.exponent(), 0);
    assert_eq!(2.0_f32.exponent(), 1);
    assert_eq!(3.3333_f32.exponent(), 1);
    assert_eq!(0.3333_f32.exponent(), -2);
    assert_eq!(31.0_f32.exponent(), 4);
  }

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
      let int = x.int_float_to_latent();
      let recovered = f32::int_float_from_latent(int);
      // gotta compare unsigneds because floats don't implement Equal
      assert_eq!(
        x.to_bits(),
        recovered.to_bits(),
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
      let int = x.int_float_to_latent();
      let recovered = f64::int_float_from_latent(int);
      // gotta compare unsigneds because floats don't implement Equal
      assert_eq!(
        x.to_bits(),
        recovered.to_bits(),
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
      (1 << 24) as f32,
      f32::MAX,
      f32::INFINITY,
      f32::NAN,
    ];
    let mut last_int = None;
    for x in values {
      let int = x.int_float_to_latent();
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
  }
}
