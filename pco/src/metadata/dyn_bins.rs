use crate::data_types::Latent;
use crate::macros::{define_latent_enum, match_latent_enum};
use crate::metadata::bins::Bins;

define_latent_enum!(
  #[derive(Clone, Debug, PartialEq)]
  pub DynBins,
  Bins
);

impl DynBins {
  pub(crate) fn len(&self) -> usize {
    match_latent_enum!(
      self,
      DynBins<L>(inner) => { inner.len() }
    )
  }
}
