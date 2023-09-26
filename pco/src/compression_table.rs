use crate::bin::BinCompressionInfo;
use crate::constants::Weight;
use crate::data_types::UnsignedLike;
use crate::errors::{PcoError, PcoResult};

#[derive(Debug, Clone)]
pub struct CompressionTable<U: UnsignedLike> {
  pub search_size_log: usize,
  pub search_lowers: Vec<U>,
  pub infos: Vec<BinCompressionInfo<U>>
}

impl<U: UnsignedLike> From<Vec<BinCompressionInfo<U>>> for CompressionTable<U> {
  fn from(mut infos: Vec<BinCompressionInfo<U>>) -> Self {
    infos.sort_unstable_by_key(|info| info.lower);

    let search_size_log = if infos.len() <= 1 {
      0
    } else {
      1 + (infos.len() - 1).ilog2() as usize
    };
    infos.sort_unstable_by_key(|info| info.lower);
    let mut search_lowers = infos.iter().map(|info| info.lower).collect::<Vec<_>>();
    while search_lowers.len() < (1 << search_size_log) {
      search_lowers.push(U::MAX);
    }

    Self {
      search_size_log,
      search_lowers,
      infos,
    }
  }
}
