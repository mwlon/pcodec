use crate::constants::Bitlen;
use crate::data_types::{Latent, Number};
use crate::macros::{define_latent_enum, define_number_enum, match_latent_enum};

define_number_enum!(
  #[derive(Clone, Copy, Debug, PartialEq, Eq)]
  #[repr(u8)]
  pub NumberType = NUMBER_TYPE_BYTE
);

define_latent_enum!(
  #[derive(Clone, Copy, Debug, PartialEq, Eq)]
  pub LatentType
);

// impl LatentType {
//   pub(crate) fn bits(&self) -> Bitlen {
//     match_latent_enum!(
//       self,
//       LatentType<L> => { L::BITS }
//     )
//   }
// }
