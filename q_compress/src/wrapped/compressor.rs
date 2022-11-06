use crate::ChunkMetadata;
use crate::base_compressor::BaseCompressor;
use crate::chunk_metadata::ChunkSpec;
use crate::data_types::NumberLike;
use crate::errors::QCompressResult;
use crate::mode::Wrapped;

pub type Compressor<T> = BaseCompressor<T, Wrapped>;

impl<T: NumberLike> Compressor<T> {
  /// TODO: documentation
  pub fn chunk_metadata(
    &mut self,
    nums: &[T],
    spec: &ChunkSpec,
  ) -> QCompressResult<ChunkMetadata<T>> {
    self.chunk_metadata_internal(nums, spec)
  }

  /// TODO documentation
  pub fn data_page(&mut self) -> QCompressResult<bool> {
    self.data_page_internal()
  }
}
