use std::cmp::{max, min};
use std::fmt::Debug;
use std::marker::PhantomData;

use crate::{BitWriter, Flags, huffman_encoding};
use crate::chunk_metadata::{ChunkMetadata, PrefixMetadata};
use crate::compression_table::CompressionTable;
use crate::constants::*;
use crate::data_types::{NumberLike, UnsignedLike};
use crate::delta_encoding;
use crate::delta_encoding::DeltaMoments;
use crate::errors::{QCompressError, QCompressResult};
use crate::prefix::{Prefix, PrefixCompressionInfo, WeightedPrefix};
use crate::prefix_optimization;

const DEFAULT_COMPRESSION_LEVEL: usize = 6;
const MIN_N_TO_USE_RUN_LEN: usize = 1001;
const MIN_FREQUENCY_TO_USE_RUN_LEN: f64 = 0.8;
const DEFAULT_CHUNK_SIZE: usize = 1000000;

struct JumpstartConfiguration {
  weight: usize,
  jumpstart: usize,
}

/// All the settings you can specify about compression.
///
/// Some, like `delta_encoding_order`, are explicitly stored as `Flags` in the
/// compressed bytes.
/// Others, like `compression_level`, affect compression but are not explicitly
/// stored in the output.
#[derive(Clone, Debug)]
pub struct CompressorConfig {
  /// `compression_level` ranges from 0 to 12 inclusive, defaulting to 6.
  ///
  /// The compressor uses up to 2^`compression_level` prefixes.
  ///
  /// For example,
  /// * Level 0 achieves a modest amount of compression with 1 prefix and can
  /// be twice as fast as level 6.
  /// * Level 6 achieves nearly the best compression with 64 prefixes and still
  /// runs in reasonable time. In some cases, its compression ratio is 3-4x as
  /// high as level level 0's.
  /// * Level 12 can achieve a few % better compression than 6 with 4096
  /// prefixes but runs ~10x slower in many cases.
  pub compression_level: usize,
  /// `delta_encoding_order` ranges from 0 to 7 inclusive, defaulting to 0.
  ///
  /// It is the number of times to apply delta encoding
  /// before compressing. For instance, say we have the numbers
  /// `[0, 2, 2, 4, 4, 6, 6]` and consider different delta encoding orders.
  /// * 0 indicates no delta encoding, it compresses numbers
  /// as-is. This is perfect for columnar data were the order is essentially
  /// random.
  /// * 1st order delta encoding takes consecutive differences, leaving
  /// `[0, 2, 0, 2, 0, 2, 0]`. This is perfect for continuous but noisy time
  /// series data, like stock prices.
  /// * 2nd order delta encoding takes consecutive differences again,
  /// leaving `[2, -2, 2, -2, 2, -2]`. This is perfect for locally linear data,
  /// like a sequence of timestamps sampled approximately periodically.
  /// * Higher-order delta encoding is good for time series that are very
  /// smooth, like temperature or light sensor readings.
  pub delta_encoding_order: usize,
}

impl Default for CompressorConfig {
  fn default() -> Self {
    Self {
      compression_level: DEFAULT_COMPRESSION_LEVEL,
      delta_encoding_order: 0,
    }
  }
}

// InternalCompressorConfig captures all settings that don't belong in flags
// i.e. these don't get written to the resulting bytes and aren't needed for
// decoding
#[derive(Clone, Debug)]
struct InternalCompressorConfig {
  pub compression_level: usize,
}

impl From<&CompressorConfig> for InternalCompressorConfig {
  fn from(config: &CompressorConfig) -> Self {
    InternalCompressorConfig {
      compression_level: config.compression_level,
    }
  }
}

impl Default for InternalCompressorConfig {
  fn default() -> Self {
    Self::from(&CompressorConfig::default())
  }
}

fn choose_run_len_jumpstart(
  count: usize,
  n: usize,
) -> JumpstartConfiguration {
  let freq = (count as f64) / (n as f64);
  let non_freq = 1.0 - freq;
  let jumpstart = min((-non_freq.log2()).ceil() as usize, MAX_JUMPSTART);
  let expected_n_runs = (freq * non_freq * n as f64).ceil() as usize;
  JumpstartConfiguration {
    weight: expected_n_runs,
    jumpstart,
  }
}

fn push_pref<T: NumberLike>(
  seq: &mut Vec<WeightedPrefix<T>>,
  prefix_idx: &mut usize,
  i: usize,
  j: usize,
  max_n_pref: usize,
  n: usize,
  sorted: &[T::Unsigned],
) {
  let count = j - i;
  let frequency = count as f64 / n as f64;
  let new_prefix_idx = max(*prefix_idx + 1, (j * max_n_pref) / n);
  let lower = T::from_unsigned(sorted[i]);
  let upper = T::from_unsigned(sorted[j - 1]);
  if n < MIN_N_TO_USE_RUN_LEN || frequency < MIN_FREQUENCY_TO_USE_RUN_LEN || count == n {
    // The usual case - a prefix for a range that represents either 100% or
    // <=80% of the data.
    seq.push(WeightedPrefix::new(
      count,
      count,
      lower,
      upper,
      None
    ));
  } else {
    // The weird case - a range that represents almost all (but not all) the data.
    // We create extra prefixes that can describe `reps` copies of the range at once.
    let config = choose_run_len_jumpstart(count, n);
    seq.push(WeightedPrefix::new(
      count,
      config.weight,
      lower,
      upper,
      Some(config.jumpstart)
    ));
  }
  *prefix_idx = new_prefix_idx;
}

fn train_prefixes<T: NumberLike>(
  unsigneds: Vec<T::Unsigned>,
  internal_config: &InternalCompressorConfig,
  flags: &Flags,
) -> QCompressResult<Vec<Prefix<T>>> {
  if unsigneds.is_empty() {
    return Ok(Vec::new());
  }

  let comp_level = internal_config.compression_level;
  if comp_level > MAX_COMPRESSION_LEVEL {
    return Err(QCompressError::invalid_argument(format!(
      "compresion level may not exceed {} (was {})",
      MAX_COMPRESSION_LEVEL,
      comp_level,
    )));
  }
  let n = unsigneds.len();
  if n > MAX_ENTRIES {
    return Err(QCompressError::invalid_argument(format!(
      "count may not exceed {} per chunk (was {})",
      MAX_ENTRIES,
      n,
    )));
  }

  let mut sorted = unsigneds;
  sorted.sort_unstable();
  let safe_comp_level = min(comp_level, (n as f64).log2() as usize);
  let max_n_pref = 1_usize << safe_comp_level;
  let mut raw_prefs: Vec<WeightedPrefix<T>> = Vec::new();
  let pref_ptr = &mut raw_prefs;

  let mut pref_idx = 0_usize;
  let pref_idx_ptr = &mut pref_idx;

  let mut i = 0;
  let mut backup_j = 0_usize;
  for j in 0..n {
    let target_j = ((*pref_idx_ptr + 1) * n) / max_n_pref;
    if j > 0 && sorted[j] == sorted[j - 1] {
      if j >= target_j && j - target_j >= target_j - backup_j && backup_j > i {
        push_pref(pref_ptr, pref_idx_ptr, i, backup_j, max_n_pref, n, &sorted);
        i = backup_j;
      }
    } else {
      backup_j = j;
      if j >= target_j {
        push_pref(pref_ptr, pref_idx_ptr, i, j, max_n_pref, n, &sorted);
        i = j;
      }
    }
  }
  push_pref(pref_ptr, pref_idx_ptr, i, n, max_n_pref, n, &sorted);

  let mut optimized_prefs = prefix_optimization::optimize_prefixes(
    raw_prefs,
    flags,
  );

  huffman_encoding::make_huffman_code(&mut optimized_prefs);

  let prefixes = optimized_prefs.iter()
    .map(|wp| wp.prefix.clone())
    .collect();
  Ok(prefixes)
}

fn compress_offset_bits_w_prefix<Diff: UnsignedLike>(
  unsigned: Diff,
  p: &PrefixCompressionInfo<Diff>,
  writer: &mut BitWriter,
) {
  let off = unsigned - p.lower;
  writer.write_diff(off, p.k);
  if off < p.only_k_bits_lower || off > p.only_k_bits_upper {
    // most significant bit, if necessary, comes last
    writer.write_one((off & (Diff::ONE << p.k)) > Diff::ZERO);
  }
}

#[derive(Clone)]
struct TrainedChunkCompressor<T> where T: NumberLike {
  pub table: CompressionTable<T::Unsigned>,
  // pub prefix_infos: Vec<PrefixCompressionInfo<T>>,
}

impl<T> TrainedChunkCompressor<T> where T: NumberLike {
  pub fn new(prefixes: &[Prefix<T>]) -> QCompressResult<Self> {
    let table = CompressionTable::from(prefixes);
    Ok(Self { table })
  }

  fn compress_nums(&self, unsigneds: &[T::Unsigned], writer: &mut BitWriter) -> QCompressResult<()> {
    let mut i = 0;
    while i < unsigneds.len() {
      let unsigned = unsigneds[i];
      let p = self.table.search(unsigned)?;
      writer.write_usize(p.code, p.code_len);
      match p.run_len_jumpstart {
        None => {
          compress_offset_bits_w_prefix(unsigned, p, writer);
          i += 1;
        }
        Some(jumpstart) => {
          let mut reps = 1;
          for &other in unsigneds.iter().skip(i + 1) {
            if p.contains(other) {
              reps += 1;
            } else {
              break;
            }
          }

          // we store 1 less than the number of occurrences
          // because the prefix already implies there is at least 1 occurrence
          writer.write_varint(reps - 1, jumpstart);

          for &unsigned in unsigneds.iter().skip(i).take(reps) {
            compress_offset_bits_w_prefix(unsigned, p, writer);
          }
          i += reps;
        }
      }
    }
    writer.finish_byte();
    Ok(())
  }
}

/// Converts vectors of numbers into compressed bytes.
///
/// You can use the compressor very easily:
/// ```
/// use q_compress::Compressor;
///
/// let my_nums = vec![1, 2, 3];
/// let compressor = Compressor::<i32>::default();
/// let bytes = compressor.simple_compress(&my_nums);
/// ```
/// You can also get full control over the compression process:
/// ```
/// use q_compress::{BitWriter, Compressor, CompressorConfig};
///
/// let compressor = Compressor::<i32>::from_config(CompressorConfig {
///   compression_level: 5,
///   ..Default::default()
/// });
/// let mut writer = BitWriter::default();
///
/// compressor.header(&mut writer).expect("header failure");
/// let chunk_0 = vec![1, 2, 3];
/// compressor.chunk(&chunk_0, &mut writer).expect("chunk failure");
/// let chunk_1 = vec![4, 5];
/// compressor.chunk(&chunk_1, &mut writer).expect("chunk failure");
/// compressor.footer(&mut writer).expect("footer failure");
///
/// let bytes = writer.pop();
/// ```
/// Note that in practice we would need larger chunks than this to
/// achieve good compression, preferably containing 10k-10M numbers.
#[derive(Clone, Debug)]
pub struct Compressor<T> where T: NumberLike {
  internal_config: InternalCompressorConfig,
  flags: Flags,
  phantom: PhantomData<T>,
}

impl<T: NumberLike> Default for Compressor<T> {
  fn default() -> Self {
    Self::from_config(CompressorConfig::default())
  }
}

impl<T> Compressor<T> where T: NumberLike {
  /// Creates a new compressor, given a [`CompressorConfig`].
  /// Internally, the compressor builds [`Flags`] as well as an internal
  /// configuration that doesn't show up in the output file.
  /// You can inspect the flags it chooses with [`.flags()`][Self::flags].
  pub fn from_config(config: CompressorConfig) -> Self {
    Self {
      internal_config: InternalCompressorConfig::from(&config),
      flags: Flags::from(&config),
      phantom: PhantomData,
    }
  }

  /// Returns a reference to the compressor's flags.
  pub fn flags(&self) -> &Flags {
    &self.flags
  }

  /// Writes out a header using the compressor's data type and flags.
  /// Will return an error if the writer is not at a byte-aligned position.
  ///
  /// Each .qco file must start with such a header, which contains:
  /// * a 4-byte magic header for "qco!" in ascii,
  /// * a byte for the data type (e.g. `i64` has byte 1 and `f64` has byte
  /// 5), and
  /// * bytes for the flags used to compress.
  pub fn header(&self, writer: &mut BitWriter) -> QCompressResult<()> {
    writer.write_aligned_bytes(&MAGIC_HEADER)?;
    writer.write_aligned_byte(T::HEADER_BYTE)?;
    self.flags.write(writer)
  }

  /// Writes out a chunk of data representing the provided numbers.
  /// Will return an error if the writer is not at a byte-aligned position or
  /// the slice of numbers is empty.
  ///
  /// Each chunk contains a [`ChunkMetadata`] section followed by the chunk body.
  /// The chunk body encodes the numbers passed in here.
  pub fn chunk(&self, nums: &[T], writer: &mut BitWriter) -> QCompressResult<ChunkMetadata<T>> {
    if nums.is_empty() {
      return Err(QCompressError::invalid_argument(
        "cannot compress empty chunk"
      ));
    }

    writer.write_aligned_byte(MAGIC_CHUNK_BYTE)?;

    let n = nums.len();
    let pre_meta_bit_idx = writer.bit_size();

    let order = self.flags.delta_encoding_order;
    let (mut metadata, post_meta_byte_idx) = if order == 0 {
      let unsigneds = nums.iter()
        .map(|x| x.to_unsigned())
        .collect::<Vec<_>>();
      let prefixes = train_prefixes(
        unsigneds.clone(),
        &self.internal_config,
        &self.flags,
      )?;
      let prefix_metadata = PrefixMetadata::Simple {
        prefixes: prefixes.clone(),
      };
      let metadata = ChunkMetadata {
        n,
        compressed_body_size: 0,
        prefix_metadata,
      };
      metadata.write_to(writer, &self.flags);
      let post_meta_idx = writer.byte_size();
      let chunk_compressor = TrainedChunkCompressor::new(&prefixes)?;
      chunk_compressor.compress_nums(&unsigneds, writer)?;
      (metadata, post_meta_idx)
    } else {
      let delta_moments = DeltaMoments::from(nums, order);
      let deltas = delta_encoding::nth_order_deltas(nums, order);
      let unsigneds = deltas.iter()
        .map(|x| x.to_unsigned())
        .collect::<Vec<_>>();
      let prefixes = train_prefixes(
        unsigneds.clone(),
        &self.internal_config,
        &self.flags,
      )?;
      let prefix_metadata = PrefixMetadata::Delta {
        delta_moments,
        prefixes: prefixes.clone(),
      };
      let metadata = ChunkMetadata {
        n,
        compressed_body_size: 0,
        prefix_metadata
      };
      metadata.write_to(writer, &self.flags);
      let post_meta_idx = writer.byte_size();
      let chunk_compressor = TrainedChunkCompressor::new(&prefixes)?;
      chunk_compressor.compress_nums(&unsigneds, writer)?;
      (metadata, post_meta_idx)
    };
    metadata.compressed_body_size = writer.byte_size() - post_meta_byte_idx;
    metadata.update_write_compressed_body_size(writer, pre_meta_bit_idx);
    Ok(metadata)
  }

  /// Writes out a single footer byte indicating that the .qco file has ended.
  /// Will return an error if the writer is not byte-aligned.
  pub fn footer(&self, writer: &mut BitWriter) -> QCompressResult<()> {
    writer.write_aligned_byte(MAGIC_TERMINATION_BYTE)
  }

  /// Takes in a slice of numbers and returns compressed bytes.
  pub fn simple_compress(&self, nums: &[T]) -> Vec<u8> {
    let mut writer = BitWriter::default();
    // The following unwraps are safe because the writer will be byte-aligned
    // after each step and ensure each chunk has appropriate size.
    self.header(&mut writer).unwrap();
    nums.chunks(DEFAULT_CHUNK_SIZE)
      .for_each(|chunk| {
        self.chunk(chunk, &mut writer).unwrap();
      });

    self.footer(&mut writer).unwrap();
    writer.pop()
  }
}
