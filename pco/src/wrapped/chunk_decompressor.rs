use std::io::Read;
use crate::bit_reader::BitReader;
use crate::page_metadata::PageMetadata;
use crate::ChunkMetadata;
use crate::data_types::NumberLike;
use crate::errors::PcoResult;
use crate::wrapped::PageDecompressor;

pub struct ChunkDecompressor<T: NumberLike> {
  pub(crate) meta: ChunkMetadata<T::Unsigned>,
}

impl<T: NumberLike> From<ChunkMetadata<T::Unsigned>> for ChunkDecompressor<T> {
  fn from(meta: ChunkMetadata<T::Unsigned>) -> Self {
    Self {
      meta
    }
  }
}

impl<T: NumberLike> ChunkDecompressor<T> {
  pub fn metadata(&self) -> &ChunkMetadata<T::Unsigned> {
    &self.meta
  }

  pub fn page_decompressor(&self, n: usize, bytes: &[u8]) -> PcoResult<(PageDecompressor<T>, &[u8])> {
    let mut reader = BitReader::from(bytes);
    let page_meta = PageMetadata::<T::Unsigned>::parse_from(&mut reader, &self.meta)?;
    let pd = PageDecompressor::new(
      &self,
      n,
      page_meta,
    )?;
    Ok((pd, reader.rest()))
  }
}