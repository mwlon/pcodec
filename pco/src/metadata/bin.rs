use crate::bits::bits_to_encode_offset_bits;
use crate::compression_intermediates::BinCompressionInfo;
use crate::constants::{Bitlen, Weight};
use crate::data_types::Latent;

/// Part of [`ChunkLatentVarMeta`][`crate::metadata::ChunkLatentVarMeta`] representing
/// a numerical range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bin<L: Latent> {
  /// The number of occurrences of this bin in the asymmetric numeral system
  /// table.
  pub weight: Weight,
  /// The lower bound for this bin's numerical range.
  pub lower: L,
  /// The log of the size of this bin's (inclusive) numerical range.
  pub offset_bits: Bitlen,
}

impl<L: Latent> Bin<L> {
  pub(crate) fn exact_bit_size(ans_size_log: Bitlen) -> Bitlen {
    ans_size_log + L::BITS + bits_to_encode_offset_bits::<L>()
  }

  #[inline]
  pub(crate) fn worst_case_bits_per_latent(&self, ans_size_log: Bitlen) -> Bitlen {
    self.offset_bits + ans_size_log - self.weight.ilog2()
  }
}

impl<L: Latent> From<BinCompressionInfo<L>> for Bin<L> {
  fn from(info: BinCompressionInfo<L>) -> Self {
    Bin {
      weight: info.weight,
      lower: info.lower,
      offset_bits: info.offset_bits,
    }
  }
}
