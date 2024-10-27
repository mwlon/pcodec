use crate::constants::Bitlen;
use crate::data_types::Latent;
use crate::delta;
use crate::delta::DeltaState;
use crate::per_latent_var::LatentVarKey;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeltaLz77Config {
  pub state_n_log: Bitlen,
  pub window_n_log: Bitlen,
}

impl DeltaLz77Config {
  pub(crate) fn state_n(&self) -> usize {
    1 << self.state_n_log
  }

  pub(crate) fn window_n(&self) -> usize {
    1 << self.window_n_log
  }
}

/// How Pco does
/// [delta encoding](https://en.wikipedia.org/wiki/Delta_encoding) on this
/// chunk.
///
/// Delta encoding optionally takes differences between nearby numbers,
/// greatly reducing the entropy of the data distribution in some cases.
/// This stage of processing happens after applying the
/// [`Mode`][crate::metadata::Mode].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeltaEncoding {
  /// No delta encoding; the values are encoded as-is.
  ///
  /// This is best if your data is in random order.
  None,
  /// Encodes the differences between values (or differences between those,
  /// etc.).
  ///
  /// This order is always positive, between 1 and 7.
  Consecutive(usize),
  Lz77(DeltaLz77Config),
}

impl DeltaEncoding {
  pub(crate) fn applies_to_latent_var(&self, key: LatentVarKey) -> bool {
    match key {
      // We never recursively delta encode.
      LatentVarKey::Delta => false,
      // We always apply the DeltaEncoding to the primary latents.
      LatentVarKey::Primary => true,
      // At present we never apply DeltaEncoding to the secondary latents, but
      // this could be changed in the future.
      LatentVarKey::Secondary => false,
    }
  }

  pub(crate) fn for_latent_var(self, key: LatentVarKey) -> DeltaEncoding {
    if self.applies_to_latent_var(key) {
      self
    } else {
      DeltaEncoding::None
    }
  }

  pub(crate) fn n_latents_per_state(&self) -> usize {
    match self {
      Self::None => 0,
      Self::Consecutive(order) => *order,
      Self::Lz77(config) => 1 << config.state_n_log,
    }
  }
}
