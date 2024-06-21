use std::mem;

use crate::data_types::{unsigneds, NumberLike};
use crate::describers::LatentDescriber;
use crate::{describers, int_mult_utils, ChunkMeta};
use crate::{ChunkConfig, Mode};

macro_rules! impl_signed {
  ($t: ty, $latent: ty, $header_byte: expr) => {
    impl NumberLike for $t {
      const DTYPE_BYTE: u8 = $header_byte;
      const TRANSMUTABLE_TO_LATENT: bool = true;

      type L = $latent;

      fn get_latent_describers(meta: &ChunkMeta<Self::L>) -> Vec<LatentDescriber<Self::L>> {
        describers::match_classic_mode::<Self>(meta, "")
          .or_else(|| describers::match_int_modes(meta, true))
          .expect("invalid mode for signed type")
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
      ) -> (Mode<Self::L>, Vec<Vec<Self::L>>) {
        unsigneds::choose_mode_and_split_latents(&nums, config)
      }

      #[inline]
      fn from_latent_ordered(l: Self::L) -> Self {
        (l as Self).wrapping_add(Self::MIN)
      }
      #[inline]
      fn to_latent_ordered(self) -> Self::L {
        self.wrapping_sub(Self::MIN) as $latent
      }
      fn join_latents(mode: Mode<Self::L>, primary: &mut [Self::L], secondary: &[Self::L]) {
        match mode {
          Mode::Classic => (),
          Mode::IntMult(base) => int_mult_utils::join_latents(base, primary, secondary),
          _ => unreachable!("impossible mode for signed ints"),
        }
      }

      fn transmute_to_latents(slice: &mut [Self]) -> &mut [Self::L] {
        unsafe { mem::transmute(slice) }
      }
      #[inline]
      fn transmute_to_latent(self) -> Self::L {
        unsafe { mem::transmute(self) }
      }
    }
  };
}

impl_signed!(i32, u32, 3);
impl_signed!(i64, u64, 4);
impl_signed!(i16, u16, 8);

#[cfg(test)]
mod tests {
  use crate::data_types::{Latent, NumberLike};

  #[test]
  fn test_ordering() {
    assert_eq!(i32::MIN.to_latent_ordered(), 0_u32);
    assert_eq!((-1_i32).to_latent_ordered(), u32::MID - 1);
    assert_eq!(0_i32.to_latent_ordered(), u32::MID);
    assert_eq!(i32::MAX.to_latent_ordered(), u32::MAX);
  }
}
