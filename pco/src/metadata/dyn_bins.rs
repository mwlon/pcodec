use crate::data_types::Latent;
use crate::macros::define_latent_enum;
use crate::metadata::bins::Bins;

define_latent_enum!(
  #[derive(Clone, Debug, PartialEq, Eq)]
  pub DynBins(Bins)
);
