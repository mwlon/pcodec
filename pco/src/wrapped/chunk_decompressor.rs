use crate::bit_reader::BitReader;
use crate::constants::PAGE_LATENT_VAR_META_PADDING;
use crate::data_types::NumberLike;
use crate::errors::PcoResult;
use crate::page_meta::PageMeta;
use crate::wrapped::PageDecompressor;
use crate::{bit_reader, ChunkMeta};

/// Holds metadata about a chunk and can produce page decompressors.
#[derive(Clone, Debug)]
pub struct ChunkDecompressor<T: NumberLike> {
  pub(crate) meta: ChunkMeta<T::Unsigned>,
}

impl<T: NumberLike> From<ChunkMeta<T::Unsigned>> for ChunkDecompressor<T> {
  fn from(meta: ChunkMeta<T::Unsigned>) -> Self {
    Self { meta }
  }
}

impl<T: NumberLike> ChunkDecompressor<T> {
  /// Returns pre-computed information about the chunk.
  pub fn meta(&self) -> &ChunkMeta<T::Unsigned> {
    &self.meta
  }

  /// Reads metadata for a page and returns a `PageDecompressor` and the
  /// number of bytes read.
  ///
  /// Will return an error if corruptions or insufficient data are found.
  pub fn page_decompressor(&self, n: usize, src: &[u8]) -> PcoResult<(PageDecompressor<T>, usize)> {
    let extension = bit_reader::make_extension_for(src, PAGE_LATENT_VAR_META_PADDING);
    let mut reader = BitReader::new(src, &extension);
    let page_meta = PageMeta::<T::Unsigned>::parse_from(&mut reader, &self.meta)?;
    let pd = PageDecompressor::new(self, n, page_meta, reader.bits_past_byte % 8)?;
    Ok((pd, reader.aligned_bytes_consumed()?))
  }
}
