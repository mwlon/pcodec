use crate::constants::MAX_ANS_BITS;
use crate::data_types::{Latent, NumberLike};
use crate::page_meta::PageMeta;
use crate::{bits, Bin, ChunkConfig, ChunkLatentVarMeta, ChunkMeta, Mode};

pub fn header_size() -> usize {
  1
}

pub(crate) fn baseline_chunk_meta<L: Latent>() -> ChunkMeta<L> {
  ChunkMeta {
    mode: Mode::Classic,
    delta_encoding_order: 0,
    per_latent_var: vec![ChunkLatentVarMeta {
      ans_size_log: 0,
      bins: vec![Bin {
        weight: 1,
        lower: L::ZERO,
        offset_bits: L::BITS,
      }],
    }],
  }
}

pub fn chunk_size<L: Latent>(n: usize) -> usize {
  // TODO if we ever add NumberLikes that are smaller than their Latents, we
  // may want to make this more generic
  baseline_chunk_meta::<L>().exact_size() + n * bits::ceil_div(L::BITS as usize, 8)
}
