use better_io::BetterBufRead;

use crate::data_types::NumberLike;
use crate::errors::{PcoError, PcoResult};
use crate::metadata::ChunkMeta;
use crate::wrapped::PageDecompressor;

/// Holds metadata about a chunk and can produce page decompressors.
#[derive(Clone, Debug)]
pub struct ChunkDecompressor<T: NumberLike> {
  pub(crate) meta: ChunkMeta<T::L>,
}

impl<T: NumberLike> ChunkDecompressor<T> {
  pub(crate) fn new(meta: ChunkMeta<T::L>) -> PcoResult<Self> {
    if T::mode_is_valid(meta.mode) {
      Ok(Self { meta })
    } else {
      Err(PcoError::corruption(format!(
        "invalid mode for data type: {:?}",
        meta.mode
      )))
    }
  }

  /// Returns pre-computed information about the chunk.
  pub fn meta(&self) -> &ChunkMeta<T::L> {
    &self.meta
  }

  /// Reads metadata for a page and returns a `PageDecompressor` and the
  /// remaining input.
  ///
  /// Will return an error if corruptions or insufficient data are found.
  pub fn page_decompressor<R: BetterBufRead>(
    &self,
    src: R,
    n: usize,
  ) -> PcoResult<PageDecompressor<T, R>> {
    PageDecompressor::new(src, &self.meta, n)
  }
}
