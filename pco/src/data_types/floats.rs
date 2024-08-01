use std::mem;

use half::f16;

use crate::chunk_config::ModeSpec;
use crate::constants::Bitlen;
use crate::data_types::{split_latents_classic, FloatLike, Latent, NumberLike};
use crate::describers::LatentDescriber;
use crate::errors::{PcoError, PcoResult};
use crate::float_mult_utils::FloatMultConfig;
use crate::{
  describers, float_mult_utils, float_quant_utils, mode::Bid, sampling, ChunkConfig, ChunkMeta,
  Mode,
};

use super::ModeAndLatents;

fn filter_sample<F: FloatLike>(num: &F) -> Option<F> {
  // We can compress infinities, nans, and baby floats, but we can't learn
  // the mode from them.
  if num.is_finite_and_normal() {
    let abs = num.abs();
    if abs <= F::MAX_FOR_SAMPLING {
      return Some(abs);
    }
  }
  None
}

fn choose_mode_and_split_latents<F: FloatLike>(
  nums: &[F],
  chunk_config: &ChunkConfig,
) -> PcoResult<ModeAndLatents<F::L>> {
  match chunk_config.mode_spec {
    ModeSpec::Auto => {
      // up to 3 bids: classic, float mult, float quant modes
      let mut bids: Vec<Bid<F>> = vec![];
      bids.push(Bid {
        mode: Mode::Classic,
        bits_saved_per_num: 0.0,
        split_fn: Box::new(|nums| split_latents_classic(nums)),
      });

      if let Some(sample) = sampling::choose_sample(nums, filter_sample) {
        bids.extend(float_mult_utils::compute_bid(&sample));
        bids.extend(float_quant_utils::compute_bid(&sample));
      }

      let winning_bid = choose_winning_bid(bids);
      let latents = (winning_bid.split_fn)(nums);
      Ok((winning_bid.mode, latents))
    }
    ModeSpec::Classic => Ok((Mode::Classic, split_latents_classic(nums))),
    ModeSpec::TryFloatMult(base_f64) => {
      let base = F::from_f64(base_f64);
      let mode = Mode::float_mult(base);
      let float_mult_config = FloatMultConfig {
        base,
        inv_base: base.inv(),
      };
      let latents = float_mult_utils::split_latents(nums, float_mult_config);
      Ok((mode, latents))
    }
    ModeSpec::TryFloatQuant(k) => Ok((
      Mode::FloatQuant(k),
      float_quant_utils::split_latents(nums, k),
    )),
    ModeSpec::TryIntMult(_) => Err(PcoError::invalid_argument(
      "unable to use int mult mode on floats",
    )),
  }
}

// one day we might reuse this for int modes
fn choose_winning_bid<T: NumberLike>(bids: Vec<Bid<T>>) -> Bid<T> {
  bids
    .into_iter()
    .max_by(|bid0, bid1| bid0.bits_saved_per_num.total_cmp(&bid1.bits_saved_per_num))
    .expect("bids must be nonempty")
}

macro_rules! impl_float_like {
  ($t: ty, $latent: ty, $exp_offset: expr) => {
    impl FloatLike for $t {
      /// Number of bits in the representation of the significand, excluding the implicit
      /// leading bit.  (In Rust, `MANTISSA_DIGITS` does include the implicit leading bit.)
      const PRECISION_BITS: Bitlen = Self::MANTISSA_DIGITS as Bitlen - 1;
      const ZERO: Self = 0.0;
      const MAX_FOR_SAMPLING: Self = Self::MAX * 0.5;

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
        Self::from_bits((($exp_offset + power) as $latent) << Self::PRECISION_BITS)
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
      fn is_sign_positive_(&self) -> bool {
        self.is_sign_positive()
      }

      #[inline]
      fn exponent(&self) -> i32 {
        (self.abs().to_bits() >> Self::PRECISION_BITS) as i32 - $exp_offset
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
  };
}

impl FloatLike for f16 {
  const PRECISION_BITS: Bitlen = Self::MANTISSA_DIGITS as Bitlen - 1;
  const ZERO: Self = f16::ZERO;
  const MAX_FOR_SAMPLING: Self = f16::from_bits(30719); // Half of MAX size.

  #[inline]
  fn abs(self) -> Self {
    Self::from_bits(self.to_bits() & 0x7FFF)
  }

  fn inv(self) -> Self {
    Self::ONE / self
  }

  #[inline]
  fn round(self) -> Self {
    Self::from_f32(self.to_f32().round())
  }

  #[inline]
  fn exp2(power: i32) -> Self {
    Self::from_bits(((15 + power) as u16) << Self::PRECISION_BITS)
  }

  #[inline]
  fn from_f64(x: f64) -> Self {
    Self::from_f64(x)
  }

  #[inline]
  fn to_f64(self) -> f64 {
    self.to_f64()
  }

  #[inline]
  fn is_finite_and_normal(&self) -> bool {
    self.is_finite() && self.is_normal()
  }

  #[inline]
  fn is_sign_positive_(&self) -> bool {
    self.is_sign_positive()
  }

  #[inline]
  fn exponent(&self) -> i32 {
    (self.abs().to_bits() >> Self::PRECISION_BITS) as i32 - 15
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
      Self::from_f32(abs_int as f32)
    } else {
      Self::from_bits(Self::from_f32(gpi as f32).to_bits() + (abs_int - gpi))
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
    let gpi_float = Self::from_f32(gpi as f32);
    let abs_int = if abs < gpi_float {
      abs.to_f32() as Self::L
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
    Self::from_f32(l as f32)
  }
}

macro_rules! impl_float_number_like {
  ($t: ty, $latent: ty, $sign_bit_mask: expr, $header_byte: expr) => {
    impl NumberLike for $t {
      const DTYPE_BYTE: u8 = $header_byte;
      const TRANSMUTABLE_TO_LATENT: bool = true;

      type L = $latent;

      fn get_latent_describers(meta: &ChunkMeta<Self::L>) -> Vec<LatentDescriber<Self::L>> {
        describers::match_classic_mode::<Self>(meta, " ULPs")
          .or_else(|| describers::match_float_modes::<Self>(meta))
          .expect("invalid mode for float type")
      }

      fn mode_is_valid(mode: Mode<Self::L>) -> bool {
        match mode {
          Mode::Classic => true,
          Mode::FloatMult(base_latent) => {
            Self::from_latent_ordered(base_latent).is_finite_and_normal()
          }
          Mode::FloatQuant(k) => k <= Self::PRECISION_BITS,
          _ => false,
        }
      }
      fn choose_mode_and_split_latents(
        nums: &[Self],
        config: &ChunkConfig,
      ) -> PcoResult<ModeAndLatents<Self::L>> {
        choose_mode_and_split_latents(nums, config)
      }

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
      fn join_latents(mode: Mode<Self::L>, primary: &mut [Self::L], secondary: &[Self::L]) {
        match mode {
          Mode::Classic => (),
          Mode::FloatMult(base_latent) => {
            let base = Self::from_latent_ordered(base_latent);
            float_mult_utils::join_latents(base, primary, secondary)
          }
          Mode::FloatQuant(k) => float_quant_utils::join_latents::<Self>(k, primary, secondary),
          _ => unreachable!("impossible mode for floats"),
        }
      }

      fn transmute_to_latents(slice: &mut [Self]) -> &mut [Self::L] {
        unsafe { mem::transmute(slice) }
      }

      #[inline]
      fn transmute_to_latent(self) -> Self::L {
        self.to_bits()
      }
    }
  };
}

impl_float_like!(f32, u32, 127);
impl_float_like!(f64, u64, 1023);
// f16 FloatLike is implemented separately because it's non-native.
impl_float_number_like!(f32, u32, 1_u32 << 31, 5);
impl_float_number_like!(f64, u64, 1_u64 << 63, 6);
impl_float_number_like!(f16, u16, 1_u16 << 15, 9);

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_choose_mult_mode() {
    let base = 1.5;
    let nums = (0..1000).map(|i| (i as f64) * base).collect::<Vec<_>>();
    let (mode, _) = choose_mode_and_split_latents(&nums, &ChunkConfig::default()).unwrap();
    assert_eq!(
      mode,
      Mode::FloatMult(base.to_latent_ordered())
    );
  }

  #[test]
  fn test_choose_quant_mode() {
    let lowest_num_bits = 1.0_f64.to_bits();
    let k = 20;
    let nums = (0..1000)
      .map(|i| f64::from_bits(lowest_num_bits + (i << k)))
      .collect::<Vec<_>>();
    let (mode, _) = choose_mode_and_split_latents(&nums, &ChunkConfig::default()).unwrap();
    assert_eq!(mode, Mode::FloatQuant(k));
  }

  #[test]
  fn test_float_ordering() {
    assert!(f32::NEG_INFINITY.to_latent_ordered() < (-0.0_f32).to_latent_ordered());
    assert!((-0.0_f32).to_latent_ordered() < (0.0_f32).to_latent_ordered());
    assert!((0.0_f32).to_latent_ordered() < f32::INFINITY.to_latent_ordered());
  }

  #[test]
  fn test_exponent() {
    assert_eq!(1.0_f32.exponent(), 0);
    assert_eq!(1.0_f64.exponent(), 0);
    assert_eq!(2.0_f32.exponent(), 1);
    assert_eq!(3.3333_f32.exponent(), 1);
    assert_eq!(0.3333_f32.exponent(), -2);
    assert_eq!(31.0_f32.exponent(), 4);
  }

  #[test]
  fn test_exp2() {
    assert_eq!(<f32 as FloatLike>::exp2(0), 1.0);
    assert_eq!(<f32 as FloatLike>::exp2(1), 2.0);
    assert_eq!(<f32 as FloatLike>::exp2(-1), 0.5);
    assert_eq!(<f32 as FloatLike>::exp2(2), 4.0);

    assert_eq!(<f16 as FloatLike>::exp2(0), f16::ONE);
    assert_eq!(<f64 as FloatLike>::exp2(0), 1.0);
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
