use std::fmt::Debug;

use crate::bit_reader::{BitReader};
use crate::wrapped::chunk_decompressor::ChunkDecompressor;
use crate::chunk_metadata::ChunkMetadata;
use crate::constants::MINIMAL_PADDING_BYTES;
use crate::data_types::NumberLike;

use crate::errors::{PcoError, PcoResult};
use crate::format_version::FormatVersion;

// #[derive(Clone, Debug, Default)]
// pub struct State<T: NumberLike> {
//   pub bit_idx: usize,
//   pub flags: Option<FormatVersion>,
//   pub chunk_meta: Option<ChunkMetadata<T::Unsigned>>,
//   pub page_decompressor: Option<PageDecompressor<T>>,
//   pub terminated: bool,
// }

// fn header_dirty<T: NumberLike>(reader: &mut BitReader, use_wrapped_mode: bool) -> PcoResult<Flags> {
//   let bytes = reader.read_aligned_bytes(MAGIC_HEADER.len())?;
//   if bytes != MAGIC_HEADER {
//     return Err(PcoError::corruption(format!(
//       "magic header does not match {:?}; instead found {:?}",
//       MAGIC_HEADER, bytes,
//     )));
//   }
//   let bytes = reader.read_aligned_bytes(1)?;
//   let byte = bytes[0];
//   if byte != T::HEADER_BYTE {
//     return Err(PcoError::corruption(format!(
//       "data type byte does not match {:?}; instead found {:?}",
//       T::HEADER_BYTE,
//       byte,
//     )));
//   }
//
//   let res = Flags::parse_from(reader)?;
//   res.check_mode(use_wrapped_mode)?;
//   Ok(res)
// }

// impl<T: NumberLike> State<T> {
//   pub fn check_step(&self, expected: Step, desc: &'static str) -> PcoResult<()> {
//     self.check_step_among(&[expected], desc)
//   }
//
//   pub fn check_step_among(&self, expected: &[Step], desc: &'static str) -> PcoResult<()> {
//     let step = self.step();
//     if expected.contains(&step) {
//       Ok(())
//     } else {
//       Err(step.wrong_step_err(desc))
//     }
//   }
//
//   pub fn chunk_meta_option_dirty(
//     &self,
//     reader: &mut BitReader,
//   ) -> PcoResult<Option<ChunkMetadata<T::Unsigned>>> {
//     let magic_byte = reader.read_aligned_bytes(1)?[0];
//     if magic_byte == MAGIC_TERMINATION_BYTE {
//       return Ok(None);
//     } else if magic_byte != MAGIC_CHUNK_BYTE {
//       return Err(PcoError::corruption(format!(
//         "invalid magic chunk byte: {}",
//         magic_byte
//       )));
//     }
//
//     ChunkMetadata::<T::Unsigned>::parse_from(reader, self.flags.as_ref().unwrap()).map(Some)
//   }
//
//   pub fn new_page_decompressor(
//     &self,
//     reader: &mut BitReader,
//     n: usize,
//     compressed_page_size: usize,
//   ) -> PcoResult<PageDecompressor<T>> {
//     let start_bit_idx = reader.bit_idx();
//     let res = self.new_page_decompressor_dirty(reader, n, compressed_page_size);
//
//     if res.is_err() {
//       reader.seek_to(start_bit_idx);
//     }
//     res
//   }
//
//   fn new_page_decompressor_dirty(
//     &self,
//     reader: &mut BitReader,
//     n: usize,
//     compressed_page_size: usize,
//   ) -> PcoResult<PageDecompressor<T>> {
//     let chunk_meta = self.chunk_meta.as_ref().unwrap();
//
//     let start_byte_idx = reader.aligned_byte_idx()?;
//     let page_meta = PageMetadata::parse_from(reader, chunk_meta)?;
//     let end_byte_idx = reader.aligned_byte_idx()?;
//
//     let compressed_body_size = compressed_page_size
//       .checked_sub(end_byte_idx - start_byte_idx)
//       .ok_or_else(|| {
//         PcoError::corruption("compressed page size {} is less than data page metadata size")
//       })?;
//
//     PageDecompressor::new(
//       n,
//       compressed_body_size,
//       chunk_meta,
//       page_meta,
//     )
//   }
//
//   pub fn step(&self) -> Step {
//     if self.flags.is_none() {
//       Step::PreHeader
//     } else if self.terminated {
//       Step::Terminated
//     } else if self.chunk_meta.is_none() {
//       Step::StartOfChunk
//     } else if self.page_decompressor.is_none() {
//       Step::StartOfPage
//     } else {
//       Step::MidPage
//     }
//   }
// }

// #[derive(Clone, Copy, Debug, PartialEq, Eq)]
// pub enum Step {
//   PreHeader,
//   StartOfChunk,
//   StartOfPage,
//   MidPage,
//   Terminated,
// }

// impl Step {
//   fn wrong_step_err(&self, description: &str) -> PcoError {
//     let step_str = match self {
//       Step::PreHeader => "has not yet parsed header",
//       Step::StartOfChunk => "is at the start of a chunk",
//       Step::StartOfPage => "is at the start of a data page",
//       Step::MidPage => "is mid-data-page",
//       Step::Terminated => "has already parsed the footer",
//     };
//     PcoError::invalid_argument(format!(
//       "attempted to {} when compressor {}",
//       description, step_str,
//     ))
//   }
// }

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct FileDecompressor {
  format_version: FormatVersion,
}

impl FileDecompressor {
  pub fn new(src: &[u8]) -> PcoResult<(Self, &[u8])> {
    let mut reader = BitReader::from(src);
    reader.ensure_padded(1);
    let format_version = FormatVersion::parse_from(&mut reader)?;
    reader.check_in_bounds()?;
    Ok((Self { format_version }, reader.rest()))
  }

  pub fn format_version(&self) -> u8 {
    self.format_version.0
  }

  pub fn chunk_decompressor<T: NumberLike>(&self, bytes: &[u8]) -> PcoResult<(ChunkDecompressor<T>, &[u8])> {
    let mut reader = BitReader::from(bytes);
    let chunk_meta = ChunkMetadata::<T::Unsigned>::parse_from(&mut reader, &self.format_version)?;
    let cd = ChunkDecompressor::from(chunk_meta);
    Ok((cd, reader.rest()))
  }
  // pub fn from_config(config: DecompressorConfig) -> Self {
  //   Self {
  //     config,
  //     ..Default::default()
  //   }
  // }

  // pub fn bit_idx(&self) -> usize {
  //   self.state.bit_idx
  // }
  //
  // // this only ensures atomicity on the reader, not the state
  // // so we have to be careful to only modify state after everything else
  // // succeeds, or manually handle rolling it back
  // pub fn with_reader<X, F>(&mut self, f: F) -> PcoResult<X>
  // where
  //   F: FnOnce(&mut BitReader, &mut State<T>, &DecompressorConfig) -> PcoResult<X>,
  // {
  //   let mut reader = BitReader::from(&self.words);
  //   reader.seek_to(self.state.bit_idx);
  //   let res = f(&mut reader, &mut self.state, &self.config);
  //   if res.is_ok() {
  //     self.state.bit_idx = reader.bit_idx();
  //   }
  //   res
  // }
  //
  // pub fn header(&mut self, use_wrapped_mode: bool) -> PcoResult<Flags> {
  //   self.state.check_step(Step::PreHeader, "read header")?;
  //
  //   self.with_reader(|reader, state, _| {
  //     let flags = header_dirty::<T>(reader, use_wrapped_mode)?;
  //     state.flags = Some(flags.clone());
  //     Ok(flags)
  //   })
  // }
  //
  // pub fn page_internal(
  //   &mut self,
  //   n: usize,
  //   compressed_page_size: usize,
  //   dest: &mut [T],
  // ) -> PcoResult<()> {
  //   let old_pd = self.state.page_decompressor.clone();
  //   self.with_reader(|reader, state, _| {
  //     let mut pd = state.new_page_decompressor(reader, n, compressed_page_size)?;
  //     let res = pd.decompress(reader, dest);
  //     // we need to roll back the body decompressor if this failed
  //     state.page_decompressor = if res.is_ok() { None } else { old_pd };
  //     res?;
  //     Ok(())
  //   })
  // }
  //
  // pub fn free_compressed_memory(&mut self) {
  //   let bytes_to_free = self.state.bit_idx / 8;
  //   if bytes_to_free > 0 {
  //     self.words.truncate_left(bytes_to_free);
  //     self.state.bit_idx -= bytes_to_free * 8;
  //   }
  // }
}
