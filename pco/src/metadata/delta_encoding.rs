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
}

impl DeltaEncoding {
  pub(crate) fn n_latents_per_state(&self) -> usize {
    match self {
      Self::None => 0,
      Self::Consecutive(order) => *order,
    }
  }
}
