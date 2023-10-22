use std::io::Read;
use crate::bit_reader::BitReader;
use crate::data_types::{NumberLike, UnsignedLike};
use crate::errors::{PcoError, PcoResult};
use crate::standalone::constants::{BITS_TO_ENCODE_COMPRESSED_BODY_SIZE, BITS_TO_ENCODE_N_ENTRIES, MAGIC_HEADER, MAGIC_TERMINATION_BYTE};
use crate::{bit_reader, ChunkMetadata, wrapped};
use crate::constants::MINIMAL_PADDING_BYTES;
use crate::page_metadata::PageMetadata;
use crate::progress::Progress;

pub struct FileDecompressor(wrapped::FileDecompressor);

impl FileDecompressor {
  pub fn new(src: &[u8]) -> PcoResult<(Self, &[u8])> {
    let extension = [];
    let mut reader = BitReader::new(src, &extension);
    let header = reader.read_aligned_bytes(MAGIC_HEADER.len())?;
    if header != MAGIC_HEADER {
      return Err(PcoError::corruption(format!(
        "magic header does not match {:?}; instead found {:?}",
        MAGIC_HEADER, header,
      )));
    }
    let consumed = reader.bytes_consumed()?;

    let (inner, rest) = wrapped::FileDecompressor::new(&src[consumed..])?;
    Ok((Self(inner), rest))
  }

  pub fn format_version(&self) -> u8 {
    self.0.format_version()
  }

  pub fn chunk_decompressor<'a, T: NumberLike>(&self, src: &'a [u8]) -> PcoResult<(Option<ChunkDecompressor<T>>, &'a [u8])> {
    let extension = bit_reader::make_extension_for(src, MINIMAL_PADDING_BYTES);
    let mut reader = BitReader::new(src, &extension);
    let dtype_or_termination_byte = reader.read_aligned_bytes(1)?[0];

    if dtype_or_termination_byte == MAGIC_TERMINATION_BYTE {
      let consumed = reader.bytes_consumed()?;
      return Ok((None, &src[consumed..]))
    }

    if dtype_or_termination_byte != T::DTYPE_BYTE {
      return Err(PcoError::corruption(format!(
        "data type byte does not match {:?}; instead found {:?}",
        T::DTYPE_BYTE,
        dtype_or_termination_byte,
      )));
    }

    let n = reader.read_usize(BITS_TO_ENCODE_N_ENTRIES) + 1;
    let compressed_body_size = reader.read_usize(BITS_TO_ENCODE_COMPRESSED_BODY_SIZE);
    reader.drain_empty_byte("expected empty bits at end of standalone chunk preamble")?;
    let consumed = reader.bytes_consumed()?;
    let src = &src[consumed..];
    let (inner_cd, src) = self.0.chunk_decompressor::<T>(src)?;
    let (inner_pd, src) = inner_cd.page_decompressor(n, src)?;
    let res = ChunkDecompressor {
      inner_cd,
      inner_pd,
      n,
      n_processed: 0,
      compressed_body_size,
      n_bytes_processed: 0,
    };
    Ok((Some(res), src))
  }
}

pub struct ChunkDecompressor<T: NumberLike> {
  inner_cd: wrapped::ChunkDecompressor<T>,
  inner_pd: wrapped::PageDecompressor<T>,
  n: usize,
  n_processed: usize,
  compressed_body_size: usize,
  n_bytes_processed: usize,
}

impl<T: NumberLike> ChunkDecompressor<T> {
  pub fn metadata(&self) -> &ChunkMetadata<T::Unsigned> {
    &self.inner_cd.meta
  }

  pub fn n(&self) -> usize {
    self.n
  }

  pub fn compressed_body_size(&self) -> usize {
    self.compressed_body_size
  }

  pub fn decompress<'a>(&mut self, bytes: &'a [u8], dst: &mut [T]) -> PcoResult<(Progress, &'a [u8])> {
    let (progress, rest) = self.inner_pd.decompress(bytes, dst)?;

    self.n_processed += progress.n_processed;
    self.n_bytes_processed += bytes.len() - rest.len();

    if self.n_processed >= self.n && self.n_bytes_processed != self.compressed_body_size {
      return Err(PcoError::corruption(format!(
        "Expected {} bytes in data page but read {} by the end",
        self.compressed_body_size,
        self.n_bytes_processed,
      )));
    } else if self.n_bytes_processed > self.compressed_body_size {
      return Err(PcoError::corruption(format!(
        "Expected {} bytes in data page but read {} before reaching the end",
        self.compressed_body_size,
        self.n_bytes_processed,
      )));
    }

    Ok((progress, rest))
  }

  // a helper for some internal things
  pub(crate) fn decompress_remaining_extend<'a>(&mut self, bytes: &'a [u8], dst: &mut Vec<T>) -> PcoResult<&'a [u8]> {
    let initial_len = dst.len();
    let remaining = self.n - self.n_processed;
    dst.reserve(remaining);
    unsafe {
      dst.set_len(initial_len + remaining);
    }
    let (progress, rest) = self.decompress(bytes, &mut dst[initial_len..])?;
    assert!(progress.finished_page);
    Ok(rest)
  }
}


// use std::cmp::min;
// use std::io::Write;
//
// use crate::wrapped::file_decompressor::{FileDecompressor, State, Step};
// use crate::bit_reader::BitReader;
// use crate::data_types::NumberLike;
// use crate::errors::{ErrorKind, PcoError, PcoResult};
// use crate::page_decompressor::PageDecompressor;
// use crate::progress::Progress;
// use crate::{constants, ChunkMetadata, DecompressorConfig, FormatVersion};
//
// /// Converts .pco compressed bytes into [`FormatVersion`],
// /// [`ChunkMetadata`], and vectors of numbers.
// ///
// /// Most `Decompressor` methods leave its state unchanged if they return an
// /// error.
// ///
// /// You can use the standalone decompressor at a chunk or batch level.
// /// ```
// /// use std::io::Write;
// /// use pco::standalone::{DecompressedItem, Decompressor};
// /// use pco::DecompressorConfig;
// ///
// /// let my_bytes = vec![112, 99, 111, 33, 3, 0, 46];
// /// let mut dest = Vec::<i32>::new(); // where decompressed numbers go
// ///
// /// // DECOMPRESS BY CHUNK
// /// let mut decompressor = Decompressor::<i32>::default();
// /// decompressor.write_all(&my_bytes).unwrap();
// /// let flags = decompressor.header().expect("header");
// /// let maybe_chunk_0_meta = decompressor.chunk_metadata().expect("chunk meta");
// /// if maybe_chunk_0_meta.is_some() {
// ///   let chunk_0_nums = decompressor.chunk_body(&mut dest).expect("chunk body");
// /// }
// ///
// /// // DECOMPRESS BY BATCH
// /// let mut decompressor = Decompressor::<i32>::default();
// /// decompressor.write_all(&my_bytes).unwrap();
// /// for item in &mut decompressor {
// ///   match item.expect("stream") {
// ///     DecompressedItem::Numbers(nums) => println!("nums: {:?}", nums),
// ///     _ => (),
// ///   }
// /// }
// /// ```
// #[derive(Clone, Debug, Default)]
// pub struct Decompressor<T: NumberLike>(FileDecompressor<T>);
//
// /// The different types of data encountered when iterating through the
// /// decompressor.
// #[derive(Clone, Debug)]
// pub enum DecompressedItem<T: NumberLike> {
//   Flags(FormatVersion),
//   ChunkMetadata(ChunkMetadata<T::Unsigned>),
//   Numbers(Vec<T>),
//   Footer,
// }
//
// impl<T: NumberLike> Decompressor<T> {
//   /// Creates a new decompressor, given a [`DecompressorConfig`].
//   pub fn from_config(config: DecompressorConfig) -> Self {
//     Self(FileDecompressor::<T>::from_config(config))
//   }
//
//   /// Reads the header, returning its [`FormatVersion`] and updating this
//   /// `Decompressor`'s state.
//   /// Will return an error if the decompressor has already parsed a header,
//   /// is not byte-aligned,
//   /// runs out of data,
//   /// finds flags from a newer, incompatible version of pco,
//   /// or finds any corruptions.
//   pub fn header(&mut self) -> PcoResult<FormatVersion> {
//     self.0.header(false)
//   }
//
//   /// Reads a [`ChunkMetadata`], returning it.
//   /// Will return `None` if it instead finds a termination footer
//   /// (indicating end of the file).
//   /// Will return an error if the decompressor has not parsed the header,
//   /// has not finished the last chunk body,
//   /// is not byte-aligned,
//   /// runs out of data,
//   /// or finds any corruptions.
//   pub fn chunk_metadata(&mut self) -> PcoResult<Option<ChunkMetadata<T::Unsigned>>> {
//     self
//       .0
//       .state
//       .check_step(Step::StartOfChunk, "read chunk metadata")?;
//
//     self.0.with_reader(|reader, state, _| {
//       let maybe_meta = state.chunk_meta_option_dirty(reader)?;
//       if maybe_meta.is_none() {
//         state.terminated = true;
//       }
//       state.chunk_meta = maybe_meta.clone();
//       Ok(maybe_meta)
//     })
//   }
//
//   /// Skips the chunk body, returning nothing.
//   /// Will return an error if the decompressor is not in a chunk body,
//   /// or runs out of data.
//   pub fn skip_chunk_body(&mut self) -> PcoResult<()> {
//     self.0.state.check_step_among(
//       &[Step::StartOfPage, Step::MidPage],
//       "skip chunk body",
//     )?;
//
//     let bits_remaining = match &self.0.state.page_decompressor {
//       Some(bd) => bd.bits_remaining(),
//       None => {
//         let meta = self.0.state.chunk_meta.as_ref().unwrap();
//         meta.compressed_body_size * 8
//       }
//     };
//
//     let skipped_bit_idx = self.0.state.bit_idx + bits_remaining;
//     if skipped_bit_idx <= self.0.words.total_bits() {
//       self.0.state.bit_idx = skipped_bit_idx;
//       self.0.state.chunk_meta = None;
//       self.0.state.page_decompressor = None;
//       Ok(())
//     } else {
//       Err(PcoError::insufficient_data(format!(
//         "unable to skip chunk body to bit index {} when only {} bits available",
//         skipped_bit_idx,
//         self.0.words.total_bits(),
//       )))
//     }
//   }
//
//   /// Reads a chunk body, pushing them onto the provided vector.
//   /// Will return an error if the decompressor is not in a chunk body,
//   /// runs out of data,
//   /// or finds any corruptions.
//   pub fn chunk_body(&mut self, dest: &mut [T]) -> PcoResult<()> {
//     self
//       .0
//       .state
//       .check_step(Step::StartOfPage, "read chunk body")?;
//     let &ChunkMetadata {
//       n,
//       compressed_body_size,
//       ..
//     } = self.0.state.chunk_meta.as_ref().unwrap();
//     self.0.page_internal(n, compressed_body_size, dest)?;
//     self.0.state.chunk_meta = None;
//     Ok(())
//   }
//
//   /// Frees memory used for storing compressed bytes the decompressor has
//   /// already decoded.
//   /// Note that calling this too frequently can cause performance issues.
//   pub fn free_compressed_memory(&mut self) {
//     self.0.free_compressed_memory()
//   }
//
//   /// Returns the current bit position into the compressed data the
//   /// decompressor is pointed at.
//   /// Note that when memory is freed, this will decrease.
//   pub fn bit_idx(&self) -> usize {
//     self.0.bit_idx()
//   }
// }
//
// fn next_nums<T: NumberLike>(
//   reader: &mut BitReader,
//   pd: &mut PageDecompressor<T>,
// ) -> PcoResult<Option<(Progress, Vec<T>)>> {
//   let mut dest = vec![T::default(); min(constants::FULL_BATCH_SIZE, pd.n_remaining())];
//   match pd.decompress(reader, &mut dest) {
//     Ok(progress) => Ok(Some((progress, dest))),
//     Err(e) => {
//       if matches!(e.kind, ErrorKind::InsufficientData) {
//         Ok(None)
//       } else {
//         Err(e)
//       }
//     }
//   }
// }
//
// fn apply_nums<T: NumberLike>(
//   state: &mut State<T>,
//   dest: Vec<T>,
//   progress: Progress,
// ) -> Option<DecompressedItem<T>> {
//   if progress.n_processed == 0 {
//     None
//   } else {
//     if progress.finished_page {
//       state.chunk_meta = None;
//       state.page_decompressor = None;
//     }
//     Some(DecompressedItem::Numbers(
//       dest[..progress.n_processed].to_vec(),
//     ))
//   }
// }
//
// /// Will return an error for files in wrapped mode.
// impl<T: NumberLike> Iterator for &mut Decompressor<T> {
//   type Item = PcoResult<DecompressedItem<T>>;
//
//   fn next(&mut self) -> Option<Self::Item> {
//     let res: PcoResult<Option<DecompressedItem<T>>> = match self.0.state.step() {
//       Step::PreHeader => match self.header() {
//         Ok(flags) => Ok(Some(DecompressedItem::Flags(flags))),
//         Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => Ok(None),
//         Err(e) => Err(e),
//       },
//       Step::StartOfChunk => match self.chunk_metadata() {
//         Ok(Some(meta)) => Ok(Some(DecompressedItem::ChunkMetadata(meta))),
//         Ok(None) => Ok(Some(DecompressedItem::Footer)),
//         Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => Ok(None),
//         Err(e) => Err(e),
//       },
//       Step::StartOfPage => self.0.with_reader(|reader, state, _config| {
//         let &ChunkMetadata {
//           n,
//           compressed_body_size,
//           ..
//         } = state.chunk_meta.as_ref().unwrap();
//         let maybe_pd = state.new_page_decompressor(reader, n, compressed_body_size);
//         if let Err(e) = &maybe_pd {
//           if matches!(e.kind, ErrorKind::InsufficientData) {
//             return Ok(None);
//           }
//         }
//         let mut pd = maybe_pd?;
//         match next_nums(reader, &mut pd)? {
//           Some((progress, dest)) => {
//             state.page_decompressor = Some(pd);
//             Ok(apply_nums(state, dest, progress))
//           }
//           None => Ok(None),
//         }
//       }),
//       Step::MidPage => self.0.with_reader(|reader, state, _config| {
//         match next_nums(
//           reader,
//           state.page_decompressor.as_mut().unwrap(),
//         )? {
//           Some((progress, dest)) => Ok(apply_nums(state, dest, progress)),
//           None => Ok(None),
//         }
//       }),
//       Step::Terminated => Ok(None),
//     };
//
//     match res {
//       Ok(Some(x)) => Some(Ok(x)),
//       Ok(None) => None,
//       Err(e) => Some(Err(e)),
//     }
//   }
// }
//
// impl<T: NumberLike> Write for Decompressor<T> {
//   fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
//     self.0.write(buf)
//   }
//
//   fn flush(&mut self) -> std::io::Result<()> {
//     self.0.flush()
//   }
// }
