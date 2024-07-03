use crate::constants::Bitlen;
use crate::data_types::{split_latents_classic, Latent, NumberLike};
use crate::describers::LatentDescriber;
use crate::errors::{PcoError, PcoResult};
use crate::Mode::Classic;
use crate::{describers, int_mult_utils, ChunkConfig, ChunkMeta, Mode, ModeSpec};

use super::ModeAndLatents;

pub fn choose_mode_and_split_latents<T: NumberLike>(
  nums: &[T],
  config: &ChunkConfig,
) -> PcoResult<ModeAndLatents<T::L>> {
  match config.mode_spec {
    ModeSpec::Auto => {
      if let Some(base) = int_mult_utils::choose_base(nums) {
        let mode = Mode::IntMult(base);
        let latents = int_mult_utils::split_latents(nums, base);
        Ok((mode, latents))
      } else {
        Ok((Classic, split_latents_classic(nums)))
      }
    }

    ModeSpec::Classic => Ok((Mode::Classic, split_latents_classic(nums))),
    ModeSpec::TryFloatMult(_) | ModeSpec::TryFloatQuant(_) => Err(PcoError::invalid_argument(
      "unable to use float mode for ints",
    )),
    ModeSpec::TryIntMult(base_u64) => {
      let base = T::L::from_u64(base_u64);
      let mode = Mode::IntMult(base);
      let latents = int_mult_utils::split_latents(nums, base);
      Ok((mode, latents))
    }
  }
}

macro_rules! impl_latent {
  ($t: ty) => {
    impl Latent for $t {
      const ZERO: Self = 0;
      const ONE: Self = 1;
      const MID: Self = 1 << (Self::BITS - 1);
      const MAX: Self = Self::MAX;
      const BITS: Bitlen = Self::BITS as Bitlen;

      #[inline]
      fn from_u64(x: u64) -> Self {
        x as Self
      }

      #[inline]
      fn leading_zeros(self) -> Bitlen {
        self.leading_zeros() as Bitlen
      }

      #[inline]
      fn to_u64(self) -> u64 {
        self as u64
      }

      #[inline]
      fn wrapping_add(self, other: Self) -> Self {
        self.wrapping_add(other)
      }

      #[inline]
      fn wrapping_sub(self, other: Self) -> Self {
        self.wrapping_sub(other)
      }
    }
  };
}

impl_latent!(u16);
impl_latent!(u32);
impl_latent!(u64);

macro_rules! impl_unsigned_number {
  ($t: ty, $header_byte: expr) => {
    impl NumberLike for $t {
      const DTYPE_BYTE: u8 = $header_byte;
      const TRANSMUTABLE_TO_LATENT: bool = true;

      type L = Self;

      fn get_latent_describers(meta: &ChunkMeta<Self::L>) -> Vec<LatentDescriber<Self::L>> {
        describers::match_classic_mode::<Self>(meta, "")
          .or_else(|| describers::match_int_modes(meta, false))
          .expect("invalid mode for unsigned type")
      }

      fn mode_is_valid(mode: Mode<Self::L>) -> bool {
        match mode {
          Mode::Classic => true,
          Mode::IntMult(_) => true,
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
        l
      }
      #[inline]
      fn to_latent_ordered(self) -> Self::L {
        self
      }
      fn join_latents(mode: Mode<Self::L>, primary: &mut [Self::L], secondary: &[Self::L]) {
        match mode {
          Mode::Classic => (),
          Mode::IntMult(base) => int_mult_utils::join_latents(base, primary, secondary),
          _ => unreachable!("impossible mode for unsigned ints"),
        }
      }

      fn transmute_to_latents(slice: &mut [Self]) -> &mut [Self::L] {
        slice
      }
      #[inline]
      fn transmute_to_latent(self) -> Self::L {
        self
      }
    }
  };
}

impl_unsigned_number!(u32, 1);
impl_unsigned_number!(u64, 2);
impl_unsigned_number!(u16, 7);
