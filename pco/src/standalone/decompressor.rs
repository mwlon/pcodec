use std::io::Write;

use crate::base_decompressor::{BaseDecompressor, State, Step};
use crate::bit_reader::BitReader;
use crate::page_decompressor::PageDecompressor;
use crate::data_types::NumberLike;
use crate::errors::{ErrorKind, PcoError, PcoResult};
use crate::progress::Progress;
use crate::{ChunkMetadata, DecompressorConfig, Flags};

/// Converts .pco compressed bytes into [`Flags`],
/// [`ChunkMetadata`], and vectors of numbers.
///
/// Most `Decompressor` methods leave its state unchanged if they return an
/// error.
///
/// You can use the standalone decompressor at a chunk or streaming level.
/// ```
/// use std::io::Write;
/// use pco::standalone::{DecompressedItem, Decompressor};
/// use pco::DecompressorConfig;
///
/// let my_bytes = vec![112, 99, 111, 33, 3, 0, 46];
/// let mut dest = Vec::<i32>::new(); // where decompressed numbers go
///
/// // DECOMPRESS BY CHUNK
/// let mut decompressor = Decompressor::<i32>::default();
/// decompressor.write_all(&my_bytes).unwrap();
/// let flags = decompressor.header().expect("header");
/// let maybe_chunk_0_meta = decompressor.chunk_metadata().expect("chunk meta");
/// if maybe_chunk_0_meta.is_some() {
///   let chunk_0_nums = decompressor.chunk_body(&mut dest).expect("chunk body");
/// }
///
/// // STREAMING DECOMPRESS
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
  ChunkMetadata(ChunkMetadata<T::Unsigned>),
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
  /// Will return an error if the decompressor has already parsed a header,
  /// is not byte-aligned,
  /// runs out of data,
  /// finds flags from a newer, incompatible version of pco,
  /// or finds any corruptions.
  pub fn header(&mut self) -> PcoResult<Flags> {
    self.0.header(false)
  }

  /// Reads a [`ChunkMetadata`], returning it.
  /// Will return `None` if it instead finds a termination footer
  /// (indicating end of the file).
  /// Will return an error if the decompressor has not parsed the header,
  /// has not finished the last chunk body,
  /// is not byte-aligned,
  /// runs out of data,
  /// or finds any corruptions.
  pub fn chunk_metadata(&mut self) -> PcoResult<Option<ChunkMetadata<T::Unsigned>>> {
    self
      .0
      .state
      .check_step(Step::StartOfChunk, "read chunk metadata")?;

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
  pub fn skip_chunk_body(&mut self) -> PcoResult<()> {
    self.0.state.check_step_among(
      &[Step::StartOfDataPage, Step::MidDataPage],
      "skip chunk body",
    )?;

    let bits_remaining = match &self.0.state.page_decompressor {
      Some(bd) => bd.bits_remaining(),
      None => {
        let meta = self.0.state.chunk_meta.as_ref().unwrap();
        meta.compressed_body_size * 8
      }
    };

    let skipped_bit_idx = self.0.state.bit_idx + bits_remaining;
    if skipped_bit_idx <= self.0.words.total_bits() {
      self.0.state.bit_idx = skipped_bit_idx;
      self.0.state.chunk_meta = None;
      self.0.state.page_decompressor = None;
      Ok(())
    } else {
      Err(PcoError::insufficient_data(format!(
        "unable to skip chunk body to bit index {} when only {} bits available",
        skipped_bit_idx,
        self.0.words.total_bits(),
      )))
    }
  }

  /// Reads a chunk body, pushing them onto the provided vector.
  /// Will return an error if the decompressor is not in a chunk body,
  /// runs out of data,
  /// or finds any corruptions.
  pub fn chunk_body(&mut self, dest: &mut [T]) -> PcoResult<()> {
    self
      .0
      .state
      .check_step(Step::StartOfDataPage, "read chunk body")?;
    let &ChunkMetadata {
      n,
      compressed_body_size,
      ..
    } = self.0.state.chunk_meta.as_ref().unwrap();
    self.0.data_page_internal(n, compressed_body_size, dest)?;
    self.0.state.chunk_meta = None;
    Ok(())
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
  bd: &mut PageDecompressor<T>,
  dest: &mut [T],
) -> PcoResult<Progress> {
  bd.decompress(reader, false, dest)
}

fn apply_nums<T: NumberLike>(
  state: &mut State<T>,
  dest: Vec<T>,
  progress: Progress,
) -> Option<DecompressedItem<T>> {
  if progress.n_processed == 0 {
    None
  } else {
    if progress.finished_body {
      state.chunk_meta = None;
      state.page_decompressor = None;
    }
    Some(DecompressedItem::Numbers(
      dest[..progress.n_processed].to_vec(),
    ))
  }
}

/// Will return an error for files in wrapped mode.
impl<T: NumberLike> Iterator for &mut Decompressor<T> {
  type Item = PcoResult<DecompressedItem<T>>;

  fn next(&mut self) -> Option<Self::Item> {
    let res: PcoResult<Option<DecompressedItem<T>>> = match self.0.state.step() {
      Step::PreHeader => match self.header() {
        Ok(flags) => Ok(Some(DecompressedItem::Flags(flags))),
        Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => Ok(None),
        Err(e) => Err(e),
      },
      Step::StartOfChunk => match self.chunk_metadata() {
        Ok(Some(meta)) => Ok(Some(DecompressedItem::ChunkMetadata(meta))),
        Ok(None) => Ok(Some(DecompressedItem::Footer)),
        Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => Ok(None),
        Err(e) => Err(e),
      },
      Step::StartOfDataPage => self.0.with_reader(|reader, state, config| {
        let &ChunkMetadata {
          n,
          compressed_body_size,
          ..
        } = state.chunk_meta.as_ref().unwrap();
        let maybe_bd = state.new_page_decompressor(reader, n, compressed_body_size);
        if let Err(e) = &maybe_bd {
          if matches!(e.kind, ErrorKind::InsufficientData) {
            return Ok(None);
          }
        }
        let mut bd = maybe_bd?;
        let mut dest = vec![T::default(); config.numbers_limit_per_item];
        let progress = next_nums_dirty(reader, &mut bd, &mut dest)?;
        state.page_decompressor = Some(bd);
        Ok(apply_nums(state, dest, progress))
      }),
      Step::MidDataPage => self.0.with_reader(|reader, state, config| {
        let mut dest = vec![T::default(); config.numbers_limit_per_item];
        let progress = next_nums_dirty(
          reader,
          state.page_decompressor.as_mut().unwrap(),
          &mut dest,
        )?;
        Ok(apply_nums(state, dest, progress))
      }),
      Step::Terminated => Ok(None),
    };

    match res {
      Ok(Some(x)) => Some(Ok(x)),
      Ok(None) => None,
      Err(e) => Some(Err(e)),
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
