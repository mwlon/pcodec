use crate::bit_reader::BitReader;
use crate::constants::PAGE_LATENT_META_PADDING;
use crate::data_types::NumberLike;
use crate::errors::PcoResult;
use crate::page_metadata::PageMetadata;
use crate::wrapped::PageDecompressor;
use crate::{bit_reader, ChunkMetadata};

pub struct ChunkDecompressor<T: NumberLike> {
  pub(crate) meta: ChunkMetadata<T::Unsigned>,
}

impl<T: NumberLike> From<ChunkMetadata<T::Unsigned>> for ChunkDecompressor<T> {
  fn from(meta: ChunkMetadata<T::Unsigned>) -> Self {
    Self { meta }
  }
}

impl<T: NumberLike> ChunkDecompressor<T> {
  pub fn metadata(&self) -> &ChunkMetadata<T::Unsigned> {
    &self.meta
  }

  pub fn page_decompressor(
    &self,
    n: usize,
    src: &[u8],
  ) -> PcoResult<(PageDecompressor<T>, usize)> {
    let extension = bit_reader::make_extension_for(src, PAGE_LATENT_META_PADDING);
    let mut reader = BitReader::new(src, &extension);
    let page_meta = PageMetadata::<T::Unsigned>::parse_from(&mut reader, &self.meta)?;
    let pd = PageDecompressor::new(&self, n, page_meta)?;
    Ok((pd, reader.bytes_consumed()?))
  }
}
