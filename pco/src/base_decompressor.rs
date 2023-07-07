use std::fmt::Debug;
use std::io::Write;

use crate::bit_reader::BitReader;
use crate::bit_words::PaddedBytes;
use crate::body_decompressor::BodyDecompressor;
use crate::chunk_metadata::{ChunkMetadata, DataPageMetadata, DataPageStreamMetadata};
use crate::constants::{MAGIC_CHUNK_BYTE, MAGIC_HEADER, MAGIC_TERMINATION_BYTE};
use crate::data_types::NumberLike;
use crate::delta_encoding::DeltaMoments;
use crate::errors::{PcoError, PcoResult};
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
) -> PcoResult<Flags> {
  let bytes = reader.read_aligned_bytes(MAGIC_HEADER.len())?;
  if bytes != MAGIC_HEADER {
    return Err(PcoError::corruption(format!(
      "magic header does not match {:?}; instead found {:?}",
      MAGIC_HEADER, bytes,
    )));
  }
  let bytes = reader.read_aligned_bytes(1)?;
  let byte = bytes[0];
  if byte != T::HEADER_BYTE {
    return Err(PcoError::corruption(format!(
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
  pub fn check_step(&self, expected: Step, desc: &'static str) -> PcoResult<()> {
    self.check_step_among(&[expected], desc)
  }

  pub fn check_step_among(&self, expected: &[Step], desc: &'static str) -> PcoResult<()> {
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
  ) -> PcoResult<Option<ChunkMetadata<T::Unsigned>>> {
    let magic_byte = reader.read_aligned_bytes(1)?[0];
    if magic_byte == MAGIC_TERMINATION_BYTE {
      return Ok(None);
    } else if magic_byte != MAGIC_CHUNK_BYTE {
      return Err(PcoError::corruption(format!(
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
  ) -> PcoResult<BodyDecompressor<T>> {
    let start_bit_idx = reader.bit_idx();
    let res = self.new_body_decompressor_dirty(reader, n, compressed_page_size);

    if res.is_err() {
      reader.seek_to(start_bit_idx);
    }
    res
  }

  fn new_body_decompressor_dirty(
    &self,
    reader: &mut BitReader,
    n: usize,
    compressed_page_size: usize,
  ) -> PcoResult<BodyDecompressor<T>> {
    let flags = self.flags.as_ref().unwrap();
    let chunk_meta = self.chunk_meta.as_ref().unwrap();

    let start_byte_idx = reader.aligned_byte_idx()?;

    let mut streams = Vec::with_capacity(chunk_meta.streams.len());
    for (stream_idx, chunk_stream_meta) in chunk_meta.streams.iter().enumerate() {
      let delta_order = chunk_meta
        .mode
        .stream_delta_order(stream_idx, flags.delta_encoding_order);
      let ans_size_log = chunk_stream_meta.ans_size_log;
      let delta_moments = DeltaMoments::parse_from(reader, delta_order)?;
      let ans_final_state = (1 << ans_size_log) + reader.read_usize(ans_size_log)?;
      streams.push(DataPageStreamMetadata::new(
        chunk_stream_meta,
        delta_moments,
        ans_final_state,
      ));
    }

    reader.drain_empty_byte("non-zero bits at end of data page metadata")?;
    let end_byte_idx = reader.aligned_byte_idx()?;
    let compressed_body_size = compressed_page_size
      .checked_sub(end_byte_idx - start_byte_idx)
      .ok_or_else(|| {
        PcoError::corruption("compressed page size {} is less than data page metadata size")
      })?;

    let data_page_meta = DataPageMetadata {
      compressed_body_size,
      n,
      mode: chunk_meta.mode,
      streams,
    };

    BodyDecompressor::new(data_page_meta)
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
  fn wrong_step_err(&self, description: &str) -> PcoError {
    let step_str = match self {
      Step::PreHeader => "has not yet parsed header",
      Step::StartOfChunk => "is at the start of a chunk",
      Step::StartOfDataPage => "is at the start of a data page",
      Step::MidDataPage => "is mid-data-page",
      Step::Terminated => "has already parsed the footer",
    };
    PcoError::invalid_argument(format!(
      "attempted to {} when compressor {}",
      description, step_str,
    ))
  }
}

#[derive(Clone, Debug, Default)]
pub struct BaseDecompressor<T: NumberLike> {
  pub config: DecompressorConfig,
  pub words: PaddedBytes,
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
  pub fn with_reader<X, F>(&mut self, f: F) -> PcoResult<X>
  where
    F: FnOnce(&mut BitReader, &mut State<T>, &DecompressorConfig) -> PcoResult<X>,
  {
    let mut reader = BitReader::from(&self.words);
    reader.seek_to(self.state.bit_idx);
    let res = f(&mut reader, &mut self.state, &self.config);
    if res.is_ok() {
      self.state.bit_idx = reader.bit_idx();
    }
    res
  }

  pub fn header(&mut self, use_wrapped_mode: bool) -> PcoResult<Flags> {
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
  ) -> PcoResult<()> {
    let old_bd = self.state.body_decompressor.clone();
    self.with_reader(|reader, state, _| {
      let mut bd = state.new_body_decompressor(reader, n, compressed_page_size)?;
      let res = bd.decompress(reader, true, dest);
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