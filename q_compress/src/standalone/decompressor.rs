use crate::{ChunkMetadata, DecompressorConfig, Flags};
use crate::base_decompressor::{BaseDecompressor, begin_data_page, read_chunk_meta, read_header, State, Step};
use crate::bit_reader::BitReader;
use crate::data_types::NumberLike;
use crate::errors::{ErrorKind, QCompressError, QCompressResult};
use crate::mode::Standalone;

pub type Decompressor<T> = BaseDecompressor<T, Standalone>;

/// The different types of data encountered when iterating through the
/// decompressor.
#[derive(Clone, Debug)]
pub enum DecompressedItem<T: NumberLike> {
  Flags(Flags),
  ChunkMetadata(ChunkMetadata<T>),
  Numbers(Vec<T>),
  Footer,
}

/// Converts compressed bytes into [`Flags`], [`ChunkMetadata`],
/// and vectors of numbers.
///
/// All `Decompressor` methods leave its state unchanged if they return an
/// error.
///
/// You can use the decompressor at a file, chunk, or stream level.
/// ```
/// use std::io::Write;
/// use q_compress::standalone::{DecompressedItem, Decompressor};
/// use q_compress::DecompressorConfig;
///
/// let my_bytes = vec![113, 99, 111, 33, 3, 0, 46];
///
/// // DECOMPRESS WHOLE FILE
/// let mut decompressor = Decompressor::<i32>::default();
/// decompressor.write_all(&my_bytes).unwrap();
/// let nums: Vec<i32> = decompressor.simple_decompress().expect("decompression");
///
/// // DECOMPRESS BY CHUNK
/// let mut decompressor = Decompressor::<i32>::default();
/// decompressor.write_all(&my_bytes).unwrap();
/// let flags = decompressor.header().expect("header");
/// let maybe_chunk_0_meta = decompressor.chunk_metadata().expect("chunk meta");
/// if maybe_chunk_0_meta.is_some() {
///   let chunk_0_nums = decompressor.chunk_body().expect("chunk body");
/// }
///
/// // DECOMPRESS BY STREAM
/// let mut decompressor = Decompressor::<i32>::default();
/// decompressor.write_all(&my_bytes).unwrap();
/// for item in &mut decompressor {
///   match item.expect("stream") {
///     DecompressedItem::Numbers(nums) => println!("nums: {:?}", nums),
///     _ => (),
///   }
/// }
/// ```
impl<T: NumberLike> Decompressor<T> {
  /// Reads a [`ChunkMetadata`], returning it.
  /// Will return `None` if it instead finds a termination footer
  /// (indicating end of the .qco file).
  /// Will return an error if the decompressor has not parsed the header,
  /// has not finished the last chunk body,
  /// is not byte-aligned,
  /// runs out of data,
  /// or finds any corruptions.
  pub fn chunk_metadata(&mut self) -> QCompressResult<Option<ChunkMetadata<T>>> {
    self.state.check_step(Step::StartOfChunk, "read chunk metadata")?;

    self.chunk_metadata_internal()
  }
  // TODO
  /// Skips the chunk body, returning nothing.
  /// Will return an error if the decompressor is not in a chunk body,
  /// or runs out of data.
  pub fn skip_chunk_body(&mut self) -> QCompressResult<()> {
    self.state.check_step(Step::StartOfDataPage, "skip chunk body")?;

    let bits_remaining = match &self.state.body_decompressor {
      Some(bd) => bd.bits_remaining(),
      None => {
        let meta = self.state.chunk_meta.as_ref().unwrap();
        meta.compressed_body_size * 8
      }
    };

    let skipped_bit_idx = self.state.bit_idx + bits_remaining;
    if skipped_bit_idx <= self.words.total_bits {
      self.state.bit_idx = skipped_bit_idx;
      self.state.body_decompressor = None;
      Ok(())
    } else {
      Err(QCompressError::insufficient_data(format!(
        "unable to skip chunk body to bit index {} when only {} bits available",
        skipped_bit_idx,
        self.words.total_bits,
      )))
    }
  }

  // TODO
  /// Reads a chunk body, returning it as a vector of numbers.
  /// Will return an error if the decompressor is not in a chunk body,
  /// runs out of data,
  /// or finds any corruptions.
  pub fn chunk_body(&mut self) -> QCompressResult<Vec<T>> {
    self.state.check_step(Step::StartOfDataPage, "read chunk body")?;
    let &ChunkMetadata { n, compressed_body_size, ..} = self.state.chunk_meta.as_ref().unwrap();
    let res = self.data_page_internal(n, compressed_body_size)?;
    self.state.chunk_meta = None;
    Ok(res)
  }

  /// Takes in compressed bytes and returns a vector of numbers.
  /// Will return an error if there are any compatibility, corruption,
  /// or insufficient data issues.
  pub fn simple_decompress(&mut self) -> QCompressResult<Vec<T>> {
    // cloning/extending by a single chunk's numbers can slow down by 2%
    // so we just take ownership of the first chunk's numbers instead
    let mut res: Option<Vec<T>> = None;
    self.header()?;
    while self.chunk_metadata_internal()?.is_some() {
      let nums = self.chunk_body()?;
      res = match res {
        Some(mut existing) => {
          existing.extend(nums);
          Some(existing)
        }
        None => {
          Some(nums)
        }
      };
    }
    Ok(res.unwrap_or_default())
  }
}

fn next_nums_dirty<T: NumberLike>(
  reader: &mut BitReader,
  state: &mut State<T>,
  config: &DecompressorConfig,
) -> QCompressResult<Option<DecompressedItem<T>>> {
  let bd = state.body_decompressor.as_mut().unwrap();
  let nums_result = bd
    .decompress_next_batch(reader, config.numbers_limit_per_item, false);
  match nums_result {
    Ok(numbers) => {
      if numbers.nums.is_empty() {
        Ok(None)
      } else {
        if numbers.finished_body {
          state.body_decompressor = None;
          state.chunk_meta = None;
        }
        Ok(Some(DecompressedItem::Numbers(numbers.nums)))
      }
    }
    Err(e) => Err(e),
  }
}

fn next_dirty<T: NumberLike>(reader: &mut BitReader, state: &mut State<T>, config: &DecompressorConfig) -> QCompressResult<Option<DecompressedItem<T>>> {
  match state.step() {
    Step::PreHeader => {
      match read_header::<T, Standalone>(reader) {
        Ok(flags) => {
          state.flags = Some(flags.clone());
          Ok(Some(DecompressedItem::Flags(flags)))
        },
        Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => Ok(None),
        Err(e) => Err(e),
      }
    },
    Step::StartOfChunk => {
      match read_chunk_meta::<T>(reader, state.flags.as_ref().unwrap()) {
        Ok(Some(meta)) => {
          state.chunk_meta = Some(meta.clone());
          Ok(Some(DecompressedItem::ChunkMetadata(meta)))
        },
        Ok(None) => {
          state.terminated = true;
          Ok(Some(DecompressedItem::Footer))
        },
        Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => Ok(None),
        Err(e) => Err(e),
      }
    },
    Step::StartOfDataPage => {
      let &ChunkMetadata { n, compressed_body_size, .. } = state.chunk_meta.as_ref().unwrap();
      begin_data_page(reader, state, n, compressed_body_size, false)?;
      next_nums_dirty(reader, state, config)
    },
    Step::MidDataPage => next_nums_dirty(reader, state, config),
    Step::Terminated => Ok(None),
  }
}

/// Will return an error for files in wrapped mode.
impl<T: NumberLike> Iterator for &mut Decompressor<T> {
  type Item = QCompressResult<DecompressedItem<T>>;

  fn next(&mut self) -> Option<Self::Item> {
    let res: QCompressResult<Option<DecompressedItem<T>>> = self.with_reader(next_dirty);

    match res {
      Ok(Some(x)) => Some(Ok(x)),
      Ok(None) => None,
      Err(e) => Some(Err(e))
    }
  }
}
