use super::unsigneds;
use crate::data_types::{NumberLike, OrderedLatentConvert};
use crate::wrapped::SecondaryLatents;
use crate::{ChunkConfig, Mode};

macro_rules! impl_signed {
  ($t: ty, $latent: ty, $header_byte: expr) => {
    impl OrderedLatentConvert for $t {
      type L = $latent;

      #[inline]
      fn from_latent_ordered(l: Self::L) -> Self {
        (l as Self).wrapping_add(Self::MIN)
      }

      #[inline]
      fn to_latent_ordered(self) -> Self::L {
        self.wrapping_sub(Self::MIN) as $latent
      }
    }

    impl NumberLike for $t {
      const DTYPE_BYTE: u8 = $header_byte;

      type L = $latent;

      #[inline]
      fn is_identical(self, other: Self) -> bool {
        self == other
      }

      fn choose_mode_and_split_latents(
        nums: &[Self],
        config: &ChunkConfig,
      ) -> (Mode<Self::L>, Vec<Vec<Self::L>>) {
        unsigneds::choose_mode_and_split_latents(&nums, config)
      }
      fn join_latents(
        mode: Mode<Self::L>,
        primary: &mut [Self::L],
        secondary: SecondaryLatents<Self::L>,
        dst: &mut [Self],
      ) {
        unsigneds::join_latents(mode, primary, secondary, dst)
      }
    }
  };
}

impl_signed!(i32, u32, 3);
impl_signed!(i64, u64, 4);
