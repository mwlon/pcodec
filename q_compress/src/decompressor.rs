use std::fmt::Debug;
use std::io::Write;
use std::marker::PhantomData;

use crate::Flags;
use crate::bit_reader::BitReader;
use crate::bit_words::BitWords;
use crate::chunk_body_decompressor::ChunkBodyDecompressor;
use crate::chunk_metadata::{ChunkMetadata};
use crate::constants::{MAGIC_CHUNK_BYTE, MAGIC_HEADER, MAGIC_TERMINATION_BYTE, WORD_SIZE};
use crate::data_types::NumberLike;
use crate::errors::{ErrorKind, QCompressError, QCompressResult};

/// All configurations available for a [`Decompressor`].
#[derive(Clone, Debug)]
pub struct DecompressorConfig {
  /// The maximum number of numbers to decode at a time when streaming through
  /// the decompressor as an iterator.
  pub numbers_limit_per_item: usize,
  phantom: PhantomData<()>, // for API stability
}

impl Default for DecompressorConfig {
  fn default() -> Self {
    Self {
      numbers_limit_per_item: 100000,
      phantom: PhantomData,
    }
  }
}

impl DecompressorConfig {
  /// Sets [`numbers_limit_per_item`][DecompressorConfig::numbers_limit_per_item].
  pub fn with_numbers_limit_per_item(mut self, limit: usize) -> Self {
    self.numbers_limit_per_item = limit;
    self
  }
}

/// The different types of data encountered when iterating through the
/// decompressor.
#[derive(Clone)]
pub enum DecompressedItem<T: NumberLike> {
  Flags(Flags),
  ChunkMetadata(ChunkMetadata<T>),
  Numbers(Vec<T>),
  Footer,
}

#[derive(Clone, Default)]
struct State<T: NumberLike> {
  bit_idx: usize,
  flags: Option<Flags>,
  chunk_body_decompressor: Option<ChunkBodyDecompressor<T>>,
  terminated: bool,
}

pub(crate) fn read_header<T: NumberLike>(reader: &mut BitReader) -> QCompressResult<Flags> {
  let bytes = reader.read_aligned_bytes(MAGIC_HEADER.len())?;
  if bytes != MAGIC_HEADER {
    return Err(QCompressError::corruption(format!(
      "magic header does not match {:?}; instead found {:?}",
      MAGIC_HEADER,
      bytes,
    )));
  }
  let bytes = reader.read_aligned_bytes(1)?;
  let byte = bytes[0];
  if byte != T::HEADER_BYTE {
    return Err(QCompressError::corruption(format!(
      "data type byte does not match {:?}; instead found {:?}",
      T::HEADER_BYTE,
      byte,
    )));
  }

  Flags::parse_from(reader)
}

pub(crate) fn read_chunk_meta<T: NumberLike>(reader: &mut BitReader, flags: &Flags) -> QCompressResult<Option<ChunkMetadata<T>>> {
  let magic_byte = reader.read_aligned_bytes(1)?[0];
  if magic_byte == MAGIC_TERMINATION_BYTE {
    return Ok(None);
  } else if magic_byte != MAGIC_CHUNK_BYTE {
    return Err(QCompressError::corruption(format!(
      "invalid magic chunk byte: {}",
      magic_byte
    )));
  }

  // otherwise there is indeed another chunk
  let metadata = ChunkMetadata::parse_from(reader, flags)?;
  reader.drain_empty_byte(|| QCompressError::corruption(
    "nonzero bits in end of final byte of chunk metadata"
  ))?;

  Ok(Some(metadata))
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
/// use q_compress::{DecompressedItem, Decompressor, DecompressorConfig};
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
/// decompressor.write_all(&my_bytes);
/// let flags = decompressor.header().expect("header");
/// let maybe_chunk_0_meta = decompressor.chunk_metadata().expect("chunk meta");
/// if maybe_chunk_0_meta.is_some() {
///   let chunk_0_nums = decompressor.chunk_body().expect("chunk body");
/// }
///
/// // DECOMPRESS BY STREAM
/// let mut decompressor = Decompressor::<i32>::default();
/// decompressor.write_all(&my_bytes);
/// for item in &mut decompressor {
///   match item.expect("stream") {
///     DecompressedItem::Numbers(nums) => println!("nums: {:?}", nums),
///     _ => (),
///   }
/// }
/// ```
#[derive(Clone, Default)]
pub struct Decompressor<T> where T: NumberLike {
  config: DecompressorConfig,
  words: BitWords,
  state: State<T>,
}

impl<T: NumberLike> Write for Decompressor<T> {
  fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
    self.words.extend_bytes(buf);
    Ok(buf.len())
  }

  fn flush(&mut self) -> std::io::Result<()> {
    Ok(())
  }
}

impl<T> Decompressor<T> where T: NumberLike {
  /// Creates a new decompressor, given a [`DecompressorConfig`].
  pub fn from_config(config: DecompressorConfig) -> Self {
    Self {
      config,
      ..Default::default()
    }
  }

  /// Returns the current bit position into the compressed data the
  /// decompressor is pointed at.
  /// Note that when memory is freed, this will decrease.
  pub fn bit_idx(&self) -> usize {
    self.state.bit_idx
  }

  fn with_reader<X, F>(&mut self, f: F) -> QCompressResult<X>
  where F: FnOnce(&mut BitReader, &mut State<T>, &DecompressorConfig) -> QCompressResult<X> {
    let mut reader = BitReader::from(&self.words);
    reader.seek_to(self.state.bit_idx);
    let res = f(&mut reader, &mut self.state, &self.config);
    if res.is_ok() {
      self.state.bit_idx = reader.bit_idx();
    }
    res
  }

  fn check_not_terminated(&self) -> QCompressResult<()> {
    if self.state.terminated {
      Err(QCompressError::invalid_argument("attempted to write to terminated decompressor"))
    } else {
      Ok(())
    }
  }

  /// Reads the header, returning its [`Flags`] and updating this
  /// `Decompressor`'s state.
  /// Will return an error if this decompressor has already parsed a header,
  /// is not byte-aligned,
  /// runs out of data,
  /// finds flags from a newer, incompatible version of q_compress,
  /// or finds any corruptions.
  pub fn header(&mut self) -> QCompressResult<Flags> {
    self.check_not_terminated()?;
    if self.state.flags.is_some() {
      return Err(QCompressError::invalid_argument(
        "attempted to decompress header for the 2nd time"
      ))
    }
    self.with_reader(|reader, state, _| {
      let flags = read_header::<T>(reader)?;
      state.flags = Some(flags.clone());
      Ok(flags)
    })
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
    self.check_not_terminated()?;
    if self.state.flags.is_none() {
      return Err(QCompressError::invalid_argument(
        "attempted to decompress chunk metadata before header"
      ));
    }
    if self.state.chunk_body_decompressor.is_some() {
      return Err(QCompressError::invalid_argument(
        "attempted to decompress chunk metadata before chunk body was finished"
      ));
    }
    self.with_reader(|reader, state, _| {
      let flags = state.flags.as_ref().unwrap();
      let maybe_meta = read_chunk_meta(reader, flags)?;
      if let Some(meta) = &maybe_meta {
        state.chunk_body_decompressor = Some(ChunkBodyDecompressor::new(meta)?)
      }
      Ok(maybe_meta)
    })
  }

  fn check_in_chunk_body(&self) -> QCompressResult<()> {
    self.check_not_terminated()?;
    if self.state.chunk_body_decompressor.is_none() {
      return Err(QCompressError::invalid_argument(
        "attempted to decompress chunk body before its chunk metadata"
      ));
    }
    Ok(())
  }

  /// Skips the chunk body, returning nothing.
  /// Will return an error if the decompressor is not in a chunk body,
  /// or runs out of data.
  pub fn skip_chunk_body(&mut self) -> QCompressResult<()> {
    self.check_in_chunk_body()?;
    let cbd = self.state.chunk_body_decompressor.as_ref().unwrap();
    let skipped_bit_idx = self.state.bit_idx + cbd.bits_remaining();
    if skipped_bit_idx <= self.words.total_bits {
      self.state.bit_idx = skipped_bit_idx;
      self.state.chunk_body_decompressor = None;
      Ok(())
    } else {
      Err(QCompressError::insufficient_data(format!(
        "unable to skip chunk body to bit index {} when only {} bits available",
        skipped_bit_idx,
        self.words.total_bits,
      )))
    }
  }

  /// Reads a chunk body, returning it as a vector of numbers.
  /// Will return an error if the decompressor is not in a chunk body,
  /// runs out of data,
  /// or finds any corruptions.
  pub fn chunk_body(&mut self) -> QCompressResult<Vec<T>> {
    self.check_in_chunk_body()?;
    self.with_reader(|reader, state, _| {
      let chunk_body_decompressor = state.chunk_body_decompressor.as_mut().unwrap();
      let numbers = chunk_body_decompressor.decompress_next_batch(
        reader,
        usize::MAX,
        true,
      )?;
      state.chunk_body_decompressor = None;
      Ok(numbers.nums)
    })
  }

  /// Takes in compressed bytes and returns a vector of numbers.
  /// Will return an error if there are any compatibility, corruption,
  /// or insufficient data issues.
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
    let words_to_free = self.state.bit_idx / WORD_SIZE;
    if words_to_free > 0 {
      self.words.truncate_left(words_to_free);
      self.state.bit_idx -= words_to_free * WORD_SIZE;
    }
  }
}

impl<T: NumberLike> Iterator for &mut Decompressor<T> {
  type Item = QCompressResult<DecompressedItem<T>>;

  fn next(&mut self) -> Option<Self::Item> {
    let res = self.with_reader(|reader, state, config| {
      if state.terminated {
        return Ok(None);
      }

      if state.flags.is_none() {
        match read_header::<T>(reader) {
          Ok(flags) => {
            state.flags = Some(flags.clone());
            Ok(Some(DecompressedItem::Flags(flags)))
          },
          Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => Ok(None),
          Err(e) => Err(e),
        }
      } else if state.chunk_body_decompressor.is_none() {
        match read_chunk_meta::<T>(reader, state.flags.as_ref().unwrap()) {
          Ok(Some(meta)) => {
            match ChunkBodyDecompressor::new(&meta) {
              Ok(cbd) => {
                state.chunk_body_decompressor = Some(cbd);
                Ok(Some(DecompressedItem::ChunkMetadata(meta)))
              }
              Err(e) => Err(e)
            }
          },
          Ok(None) => {
            state.terminated = true;
            Ok(Some(DecompressedItem::Footer))
          },
          Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => Ok(None),
          Err(e) => Err(e),
        }
      } else {
        let nums_result = state.chunk_body_decompressor.as_mut()
          .unwrap()
          .decompress_next_batch(reader, config.numbers_limit_per_item, false);
        match nums_result {
          Ok(numbers) => {
            if numbers.nums.is_empty() {
              Ok(None)
            } else {
              if numbers.finished_chunk_body {
                state.chunk_body_decompressor = None;
              }
              Ok(Some(DecompressedItem::Numbers(numbers.nums)))
            }
          }
          Err(e) => Err(e),
        }
      }
    });
    match res {
      Ok(Some(x)) => Some(Ok(x)),
      Ok(None) => None,
      Err(e) => Some(Err(e))
    }
  }
}
