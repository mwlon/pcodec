use crate::bit_reader::BitReaderBuilder;
use crate::constants::PAGE_META_PADDING;
use crate::data_types::NumberLike;
use crate::errors::PcoResult;
use crate::page_meta::PageMeta;
use crate::wrapped::PageDecompressor;
use crate::ChunkMeta;
use better_io::BetterBufRead;

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
  /// remaining input.
  ///
  /// Will return an error if corruptions or insufficient data are found.
  pub fn page_decompressor<R: BetterBufRead>(
    &self,
    n: usize,
    src: R,
  ) -> PcoResult<(PageDecompressor<T>, R)> {
    let mut reader_builder = BitReaderBuilder::new(src, PAGE_META_PADDING, 0);
    let page_meta = reader_builder
      .with_reader(|reader| PageMeta::<T::Unsigned>::parse_from(reader, &self.meta))?;
    let bits_past_byte = reader_builder.bits_past_byte();
    let pd = PageDecompressor::new(self, n, page_meta, bits_past_byte)?;
    Ok((pd, reader_builder.into_inner()))
  }
}
