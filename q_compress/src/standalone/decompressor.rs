use std::io::Write;

use crate::{ChunkMetadata, DecompressorConfig, Flags};
use crate::base_decompressor::{BaseDecompressor, header_dirty, State, Step};
use crate::bit_reader::BitReader;
use crate::body_decompressor::{BodyDecompressor, Numbers};
use crate::data_types::NumberLike;
use crate::errors::{ErrorKind, QCompressError, QCompressResult};

/// Converts standalone .qco compressed bytes into [`Flags`],
/// [`ChunkMetadata`], and vectors of numbers.
///
/// Most `Decompressor` methods leave its state unchanged if they return an
/// error.
///
/// You can use the standalone decompressor at a file, chunk, or stream level.
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
#[derive(Clone, Debug, Default)]
pub struct Decompressor<T: NumberLike>(BaseDecompressor<T>);

/// The different types of data encountered when iterating through the
/// decompressor.
#[derive(Clone, Debug)]
pub enum DecompressedItem<T: NumberLike> {
  Flags(Flags),
  ChunkMetadata(ChunkMetadata<T>),
  Numbers(Vec<T>),
  Footer,
}

impl<T: NumberLike> Decompressor<T> {
  /// Creates a new decompressor, given a [`DecompressorConfig`].
  pub fn from_config(config: DecompressorConfig) -> Self {
    Self(BaseDecompressor::<T>::from_config(config))
  }

  /// Reads the header, returning its [`Flags`] and updating this
  /// `Decompressor`'s state.
  /// Will return an error if this decompressor has already parsed a header,
  /// is not byte-aligned,
  /// runs out of data,
  /// finds flags from a newer, incompatible version of q_compress,
  /// or finds any corruptions.
  pub fn header(&mut self) -> QCompressResult<Flags> {
    self.0.header(false)
  }

  /// Reads a [`ChunkMetadata`], returning it.
  /// Will return `None` if it instead finds a termination footer
  /// (indicating end of the .qco file).
  /// Will return an error if the decompressor has not parsed the header,
  /// has not finished the last chunk body,
  /// is not byte-aligned,
  /// runs out of data,
  /// or finds any corruptions.
  pub fn chunk_metadata(&mut self) -> QCompressResult<Option<ChunkMetadata<T>>> {
    self.0.state.check_step(Step::StartOfChunk, "read chunk metadata")?;

    self.0.with_reader(|reader, state, _| {
      let maybe_meta = state.chunk_meta_option_dirty(reader)?;
      if maybe_meta.is_none() {
        state.terminated = true;
      }
      state.chunk_meta = maybe_meta.clone();
      Ok(maybe_meta)
    })
  }

  /// Skips the chunk body, returning nothing.
  /// Will return an error if the decompressor is not in a chunk body,
  /// or runs out of data.
  pub fn skip_chunk_body(&mut self) -> QCompressResult<()> {
    self.0.state.check_step_among(&[Step::StartOfDataPage, Step::MidDataPage], "skip chunk body")?;

    let bits_remaining = match &self.0.state.body_decompressor {
      Some(bd) => bd.bits_remaining(),
      None => {
        let meta = self.0.state.chunk_meta.as_ref().unwrap();
        meta.compressed_body_size * 8
      }
    };

    let skipped_bit_idx = self.0.state.bit_idx + bits_remaining;
    if skipped_bit_idx <= self.0.words.total_bits {
      self.0.state.bit_idx = skipped_bit_idx;
      self.0.state.chunk_meta = None;
      self.0.state.body_decompressor = None;
      Ok(())
    } else {
      Err(QCompressError::insufficient_data(format!(
        "unable to skip chunk body to bit index {} when only {} bits available",
        skipped_bit_idx,
        self.0.words.total_bits,
      )))
    }
  }

  /// Reads a chunk body, returning it as a vector of numbers.
  /// Will return an error if the decompressor is not in a chunk body,
  /// runs out of data,
  /// or finds any corruptions.
  pub fn chunk_body(&mut self) -> QCompressResult<Vec<T>> {
    self.0.state.check_step(Step::StartOfDataPage, "read chunk body")?;
    let &ChunkMetadata { n, compressed_body_size, ..} = self.0.state.chunk_meta.as_ref().unwrap();
    let res = self.0.data_page_internal(n, compressed_body_size)?;
    self.0.state.chunk_meta = None;
    Ok(res)
  }

  // TODO in 1.0 just make this a function
  /// Takes in compressed bytes and returns a vector of numbers.
  /// Will return an error if there are any compatibility, corruption,
  /// or insufficient data issues.
  ///
  /// Unlike most methods, this does not guarantee atomicity of the
  /// compressor's state.
  pub fn simple_decompress(&mut self) -> QCompressResult<Vec<T>> {
    // cloning/extending by a single chunk's numbers can slow down by 2%
    // so we just take ownership of the first chunk's numbers instead
    let mut res: Option<Vec<T>> = None;
    self.header()?;
    while self.chunk_metadata()?.is_some() {
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

  /// Frees memory used for storing compressed bytes the decompressor has
  /// already decoded.
  /// Note that calling this too frequently can cause performance issues.
  pub fn free_compressed_memory(&mut self) {
    self.0.free_compressed_memory()
  }

  /// Returns the current bit position into the compressed data the
  /// decompressor is pointed at.
  /// Note that when memory is freed, this will decrease.
  pub fn bit_idx(&self) -> usize {
    self.0.bit_idx()
  }
}

fn next_nums_dirty<T: NumberLike>(
  reader: &mut BitReader,
  bd: &mut BodyDecompressor<T>,
  config: &DecompressorConfig,
) -> QCompressResult<Numbers<T>> {
  bd.decompress_next_batch(reader, config.numbers_limit_per_item, false)
}

fn apply_nums<T: NumberLike>(
  state: &mut State<T>,
  numbers: Numbers<T>,
) -> Option<DecompressedItem<T>> {
  if numbers.nums.is_empty() {
    None
  } else {
    if numbers.finished_body {
      state.chunk_meta = None;
      state.body_decompressor = None;
    }
    Some(DecompressedItem::Numbers(numbers.nums))
  }
}

fn next_dirty<T: NumberLike>(
  reader: &mut BitReader,
  state: &mut State<T>,
  config: &DecompressorConfig,
) -> QCompressResult<Option<DecompressedItem<T>>> {
  match state.step() {
    Step::PreHeader => {
      match header_dirty::<T>(reader, false) {
        Ok(flags) => {
          state.flags = Some(flags.clone());
          Ok(Some(DecompressedItem::Flags(flags)))
        },
        Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => Ok(None),
        Err(e) => Err(e),
      }
    },
    Step::StartOfChunk => {
      match state.chunk_meta_option_dirty(reader) {
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
      let mut bd = state.new_body_decompressor(reader, n, compressed_body_size)?;
      let numbers = next_nums_dirty(reader, &mut bd, config)?;
      state.body_decompressor = Some(bd);
      Ok(apply_nums(state, numbers))
    },
    Step::MidDataPage => {
      let numbers = next_nums_dirty(reader, state.body_decompressor.as_mut().unwrap(), config)?;
      Ok(apply_nums(state, numbers))
    },
    Step::Terminated => Ok(None),
  }
}

/// Will return an error for files in wrapped mode.
impl<T: NumberLike> Iterator for &mut Decompressor<T> {
  type Item = QCompressResult<DecompressedItem<T>>;

  fn next(&mut self) -> Option<Self::Item> {
    let res: QCompressResult<Option<DecompressedItem<T>>> = self.0.with_reader(next_dirty);

    match res {
      Ok(Some(x)) => Some(Ok(x)),
      Ok(None) => None,
      Err(e) => Some(Err(e))
    }
  }
}

impl<T: NumberLike> Write for Decompressor<T> {
  fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
    self.0.write(buf)
  }

  fn flush(&mut self) -> std::io::Result<()> {
    self.0.flush()
  }
}
