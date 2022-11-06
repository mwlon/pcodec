use crate::base_decompressor::{BaseDecompressor, Step};
use crate::ChunkMetadata;
use crate::data_types::NumberLike;
use crate::errors::QCompressResult;
use crate::mode::Wrapped;

pub type Decompressor<T> = BaseDecompressor<T, Wrapped>;

impl<T: NumberLike> Decompressor<T> {
  /// TODO
  pub fn chunk_metadata(&mut self) -> QCompressResult<Option<ChunkMetadata<T>>> {
    self.state.check_step_among(&[Step::StartOfChunk, Step::StartOfDataPage], "read chunk metadata")?;
    self.state.body_decompressor = None;

    self.chunk_metadata_internal()
  }
  // TODO
  pub fn data_page(
    &mut self,
    n: usize,
    compressed_page_size: usize,
  ) -> QCompressResult<Vec<T>> {
    self.data_page_internal(n, compressed_page_size)
  }
}
