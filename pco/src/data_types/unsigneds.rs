use crate::constants::Bitlen;
use crate::data_types::SecondaryLatents;
use crate::data_types::{Latent, NumberLike, OrderedLatentConvert};
use crate::{int_mult_utils, ChunkConfig, IntMultSpec, Mode};
use std::fmt::Display;

pub fn latent_to_string<T: OrderedLatentConvert + Display + Default>(
  l: T::L,
  mode: Mode<T::L>,
  latent_var_idx: usize,
  delta_encoding_order: usize,
) -> String {
  let format_as_signed = || {
    if l >= T::L::MID {
      (l - T::L::MID).to_string()
    } else {
      format!("-{}", T::L::MID - l)
    }
  };

  use Mode::*;
  match (mode, latent_var_idx, delta_encoding_order) {
    (Classic, 0, 0) => T::from_latent_ordered(l).to_string(),
    (IntMult(base), 0, 0) => {
      let unsigned_0 = T::default().to_latent_ordered();
      let relative_to_0 = l.wrapping_sub(unsigned_0 / base);
      T::from_latent_ordered(unsigned_0.wrapping_add(relative_to_0)).to_string()
    }
    (Classic, 0, _) | (IntMult(_), 0, _) => format_as_signed(),
    (IntMult(base), 1, _) => {
      let unsigned_0_rem = T::default().to_latent_ordered() % base;
      if l < unsigned_0_rem {
        format!("-{}", unsigned_0_rem - l)
      } else {
        (l - unsigned_0_rem).to_string()
      }
    }
    _ => panic!("invalid context for latent"),
  }
}

pub fn choose_mode_and_split_latents<T: OrderedLatentConvert>(
  nums: &[T],
  config: &ChunkConfig,
) -> (Mode<T::L>, Vec<Vec<T::L>>) {
  use IntMultSpec::*;
  let classic = || {
    let latents = vec![nums.iter().map(|x| x.to_latent_ordered()).collect()];
    (Mode::Classic, latents)
  };

  match config.int_mult_spec {
    Enabled => {
      if let Some(base) = int_mult_utils::choose_base(nums) {
        let mode = Mode::IntMult(base);
        let latents = int_mult_utils::split_latents(nums, base);
        (mode, latents)
      } else {
        classic()
      }
    }
    Provided(base_u64) => {
      let base = T::L::from_u64(base_u64);
      let mode = Mode::IntMult(base);
      let latents = int_mult_utils::split_latents(nums, base);
      (mode, latents)
    }
    Disabled => classic(),
  }
}

pub fn join_latents<T: OrderedLatentConvert>(
  mode: Mode<T::L>,
  primary: &mut [T::L],
  secondary: SecondaryLatents<T::L>,
  dst: &mut [T],
) {
  use Mode::*;
  match mode {
    IntMult(base) => int_mult_utils::join_latents::<T>(base, primary, secondary, dst),
    Classic => {
      for (&l, dst) in primary.iter().zip(dst.iter_mut()) {
        *dst = T::from_latent_ordered(l);
      }
    }
    _ => panic!("should be unreachable"),
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

impl_latent!(u32);
impl_latent!(u64);

macro_rules! impl_unsigned_number {
  ($t: ty, $header_byte: expr) => {
    impl OrderedLatentConvert for $t {
      type L = Self;

      #[inline]
      fn from_latent_ordered(l: Self::L) -> Self {
        l
      }

      #[inline]
      fn to_latent_ordered(self) -> Self::L {
        self
      }
    }

    impl NumberLike for $t {
      const DTYPE_BYTE: u8 = $header_byte;

      type L = Self;

      #[inline]
      fn is_identical(self, other: Self) -> bool {
        self == other
      }
      fn latent_to_string(
        l: Self::L,
        mode: Mode<Self::L>,
        latent_var_idx: usize,
        delta_encoding_order: usize,
      ) -> String {
        latent_to_string::<Self>(l, mode, latent_var_idx, delta_encoding_order)
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

impl_unsigned_number!(u32, 1);
impl_unsigned_number!(u64, 2);
