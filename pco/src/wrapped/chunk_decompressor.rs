use std::io::Read;
use crate::bit_reader::BitReader;
use crate::page_metadata::PageMetadata;
use crate::{bit_reader, ChunkMetadata};
use crate::constants::{MAX_ANS_BITS, MAX_DELTA_ENCODING_ORDER, MAX_SUPPORTED_PRECISION, PAGE_LATENT_META_PADDING};
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

  pub fn page_decompressor<'a>(&self, n: usize, src: &'a [u8]) -> PcoResult<(PageDecompressor<T>, &'a [u8])> {
    let extension = bit_reader::make_extension_for(src, PAGE_LATENT_META_PADDING);
    let mut reader = BitReader::new(src, &extension);
    let page_meta = PageMetadata::<T::Unsigned>::parse_from(&mut reader, &self.meta)?;
    let pd = PageDecompressor::new(
      &self,
      n,
      page_meta,
    )?;
    let consumed = reader.bytes_consumed()?;
    Ok((pd, &src[consumed..]))
  }
}