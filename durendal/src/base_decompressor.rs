use std::fmt::Debug;
use std::io::Write;

use crate::bit_reader::BitReader;
use crate::bit_words::BitWords;
use crate::body_decompressor::BodyDecompressor;
use crate::chunk_metadata::ChunkMetadata;
use crate::constants::{MAGIC_CHUNK_BYTE, MAGIC_HEADER, MAGIC_TERMINATION_BYTE};
use crate::data_types::NumberLike;
use crate::delta_encoding::DeltaMoments;
use crate::errors::{QCompressError, QCompressResult};
use crate::Flags;

/// All configurations available for a Decompressor.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct DecompressorConfig {
  /// The maximum number of numbers to decode at a time when streaming through
  /// the decompressor as an iterator.
  pub numbers_limit_per_item: usize,
}

impl Default for DecompressorConfig {
  fn default() -> Self {
    Self {
      numbers_limit_per_item: 100000,
    }
  }
}

#[derive(Clone, Debug, Default)]
pub struct State<T: NumberLike> {
  pub bit_idx: usize,
  pub flags: Option<Flags>,
  pub chunk_meta: Option<ChunkMetadata<T::Unsigned>>,
  pub body_decompressor: Option<BodyDecompressor<T>>,
  pub terminated: bool,
}

fn header_dirty<T: NumberLike>(
  reader: &mut BitReader,
  use_wrapped_mode: bool,
) -> QCompressResult<Flags> {
  let bytes = reader.read_aligned_bytes(MAGIC_HEADER.len())?;
  if bytes != MAGIC_HEADER {
    return Err(QCompressError::corruption(format!(
      "magic header does not match {:?}; instead found {:?}",
      MAGIC_HEADER, bytes,
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

  let res = Flags::parse_from(reader)?;
  res.check_mode(use_wrapped_mode)?;
  Ok(res)
}

impl<T: NumberLike> State<T> {
  pub fn check_step(&self, expected: Step, desc: &'static str) -> QCompressResult<()> {
    self.check_step_among(&[expected], desc)
  }

  pub fn check_step_among(&self, expected: &[Step], desc: &'static str) -> QCompressResult<()> {
    let step = self.step();
    if expected.contains(&step) {
      Ok(())
    } else {
      Err(step.wrong_step_err(desc))
    }
  }

  pub fn chunk_meta_option_dirty(
    &self,
    reader: &mut BitReader,
  ) -> QCompressResult<Option<ChunkMetadata<T::Unsigned>>> {
    let magic_byte = reader.read_aligned_bytes(1)?[0];
    if magic_byte == MAGIC_TERMINATION_BYTE {
      return Ok(None);
    } else if magic_byte != MAGIC_CHUNK_BYTE {
      return Err(QCompressError::corruption(format!(
        "invalid magic chunk byte: {}",
        magic_byte
      )));
    }

    ChunkMetadata::<T::Unsigned>::parse_from(reader, self.flags.as_ref().unwrap()).map(Some)
  }

  pub fn new_body_decompressor(
    &self,
    reader: &mut BitReader,
    n: usize,
    compressed_page_size: usize,
  ) -> QCompressResult<BodyDecompressor<T>> {
    let flags = self.flags.as_ref().unwrap();
    let chunk_meta = self.chunk_meta.as_ref().unwrap();

    let start_byte_idx = reader.aligned_byte_idx()?;
    let delta_moments = DeltaMoments::parse_from(reader, flags.delta_encoding_order)?;
    let end_byte_idx = reader.aligned_byte_idx()?;
    let compressed_body_size = compressed_page_size
      .checked_sub(end_byte_idx - start_byte_idx)
      .ok_or_else(|| {
        QCompressError::corruption("compressed page size {} is less than data page metadata size")
      })?;

    BodyDecompressor::new(
      &chunk_meta.bins,
      n,
      compressed_body_size,
      &delta_moments,
    )
  }

  pub fn step(&self) -> Step {
    if self.flags.is_none() {
      Step::PreHeader
    } else if self.terminated {
      Step::Terminated
    } else if self.chunk_meta.is_none() {
      Step::StartOfChunk
    } else if self.body_decompressor.is_none() {
      Step::StartOfDataPage
    } else {
      Step::MidDataPage
    }
  }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
  PreHeader,
  StartOfChunk,
  StartOfDataPage,
  MidDataPage,
  Terminated,
}

impl Step {
  fn wrong_step_err(&self, description: &str) -> QCompressError {
    let step_str = match self {
      Step::PreHeader => "has not yet parsed header",
      Step::StartOfChunk => "is at the start of a chunk",
      Step::StartOfDataPage => "is at the start of a data page",
      Step::MidDataPage => "is mid-data-page",
      Step::Terminated => "has already parsed the footer",
    };
    QCompressError::invalid_argument(format!(
      "attempted to {} when compressor {}",
      description, step_str,
    ))
  }
}

#[derive(Clone, Debug, Default)]
pub struct BaseDecompressor<T: NumberLike> {
  pub config: DecompressorConfig,
  pub words: BitWords,
  pub state: State<T>,
}

impl<T: NumberLike> Write for BaseDecompressor<T> {
  fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
    self.words.extend_bytes(buf);
    Ok(buf.len())
  }

  fn flush(&mut self) -> std::io::Result<()> {
    Ok(())
  }
}

impl<T: NumberLike> BaseDecompressor<T> {
  pub fn from_config(config: DecompressorConfig) -> Self {
    Self {
      config,
      ..Default::default()
    }
  }

  pub fn bit_idx(&self) -> usize {
    self.state.bit_idx
  }

  // this only ensures atomicity on the reader, not the state
  // so we have to be careful to only modify state after everything else
  // succeeds, or manually handle rolling it back
  pub fn with_reader<X, F>(&mut self, f: F) -> QCompressResult<X>
  where
    F: FnOnce(&mut BitReader, &mut State<T>, &DecompressorConfig) -> QCompressResult<X>,
  {
    let mut reader = BitReader::from(&self.words);
    reader.seek_to(self.state.bit_idx);
    let res = f(&mut reader, &mut self.state, &self.config);
    if res.is_ok() {
      self.state.bit_idx = reader.bit_idx();
    }
    res
  }

  pub fn header(&mut self, use_wrapped_mode: bool) -> QCompressResult<Flags> {
    self.state.check_step(Step::PreHeader, "read header")?;

    self.with_reader(|reader, state, _| {
      let flags = header_dirty::<T>(reader, use_wrapped_mode)?;
      state.flags = Some(flags.clone());
      Ok(flags)
    })
  }

  pub fn data_page_internal(
    &mut self,
    n: usize,
    compressed_page_size: usize,
    dest: &mut [T],
  ) -> QCompressResult<()> {
    let old_bd = self.state.body_decompressor.clone();
    self.with_reader(|reader, state, _| {
      let mut bd = state.new_body_decompressor(reader, n, compressed_page_size)?;
      let res = bd.decompress_next_batch(reader, true, dest);
      // we need to roll back the body decompressor if this failed
      state.body_decompressor = if res.is_ok() { None } else { old_bd };
      res?;
      Ok(())
    })
  }

  pub fn free_compressed_memory(&mut self) {
    let bytes_to_free = self.state.bit_idx / 8;
    if bytes_to_free > 0 {
      self.words.truncate_left(bytes_to_free);
      self.state.bit_idx -= bytes_to_free * 8;
    }
  }
}
