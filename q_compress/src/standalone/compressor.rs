use crate::ChunkMetadata;
use crate::base_compressor::BaseCompressor;
use crate::chunk_metadata::ChunkSpec;
use crate::data_types::NumberLike;
use crate::errors::QCompressResult;
use crate::mode::Standalone;

/// Converts vectors of numbers into compressed bytes.
///
/// All `Compressor` methods leave its state unchanged if they return an error.
/// You can configure behavior like compression level by instantiating with
/// [`.from_config()`][Compressor::from_config]
///
/// You can use the compressor at a file or chunk level.
/// ```
/// use q_compress::standalone::Compressor;
///
/// let my_nums = vec![1, 2, 3];
///
/// // FILE LEVEL
/// let mut compressor = Compressor::<i32>::default();
/// let bytes = compressor.simple_compress(&my_nums);
///
/// // CHUNK LEVEL
/// let mut compressor = Compressor::<i32>::default();
/// compressor.header().expect("header");
/// compressor.chunk(&my_nums).expect("chunk");
/// compressor.footer().expect("footer");
/// let bytes = compressor.drain_bytes();
/// ```
/// Note that in practice we would need larger chunks than this to
/// achieve good compression, preferably containing 3k-10M numbers.
pub type Compressor<T> = BaseCompressor<T, Standalone>;

const DEFAULT_CHUNK_SIZE: usize = 1000000;

impl<T: NumberLike> Compressor<T> {
  /// Writes out a chunk of data representing the provided numbers.
  /// Will return an error if the compressor has not yet written the header
  /// or already written the footer.
  ///
  /// Each chunk contains a [`ChunkMetadata`] section followed by the chunk body.
  /// The chunk body encodes the numbers passed in here.
  pub fn chunk(&mut self, nums: &[T]) -> QCompressResult<ChunkMetadata<T>> {
    let pre_meta_bit_idx = self.writer.bit_size();
    let mut meta = self.chunk_metadata_internal(nums, &ChunkSpec::default())?;
    let post_meta_byte_idx = self.writer.byte_size();

    self.data_page_internal()?;

    meta.compressed_body_size = self.writer.byte_size() - post_meta_byte_idx;
    meta.update_write_compressed_body_size(&mut self.writer, pre_meta_bit_idx);
    Ok(meta)
  }

  /// Takes in a slice of numbers and returns compressed bytes.
  pub fn simple_compress(&mut self, nums: &[T]) -> Vec<u8> {
    // The following unwraps are safe because the writer will be byte-aligned
    // after each step and ensure each chunk has appropriate size.
    self.header().unwrap();
    nums.chunks(DEFAULT_CHUNK_SIZE)
      .for_each(|chunk| {
        self.chunk(chunk).unwrap();
      });

    self.footer().unwrap();
    self.drain_bytes()
  }
}
