use crate::constants::{Bitlen, Weight};
use crate::data_types::Latent;
use crate::metadata::Bin;

pub type Bins<L> = Vec<Bin<L>>;

pub fn are_trivial<L: Latent>(bins: &[Bin<L>]) -> bool {
  bins.is_empty() || (bins.len() == 1 && bins[0].offset_bits == 0)
}

pub fn max_offset_bits<L: Latent>(bins: &[Bin<L>]) -> Bitlen {
  bins
    .iter()
    .map(|bin| bin.offset_bits)
    .max()
    .unwrap_or_default()
}

pub fn weights<L: Latent>(bins: &[Bin<L>]) -> Vec<Weight> {
  bins.iter().map(|bin| bin.weight).collect()
}

pub fn avg_bits_per_latent<L: Latent>(bins: &[Bin<L>], ans_size_log: Bitlen) -> f64 {
  let total_weight = (1 << ans_size_log) as f64;
  bins
    .iter()
    .map(|bin| {
      let ans_bits = ans_size_log as f64 - (bin.weight as f64).log2();
      (ans_bits + bin.offset_bits as f64) * bin.weight as f64 / total_weight
    })
    .sum()
}
