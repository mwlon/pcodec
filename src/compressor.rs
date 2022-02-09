use std::cmp::{max, min};
use std::fmt::Debug;
use std::marker::PhantomData;

use crate::{BitWriter, Flags, huffman_encoding};
use crate::bits::*;
use crate::chunk_metadata::{ChunkMetadata, PrefixMetadata};
use crate::constants::*;
use crate::delta_encoding;
use crate::delta_encoding::DeltaMoments;
use crate::errors::{QCompressError, QCompressResult};
use crate::prefix::{PrefixCompressionInfo, WeightedPrefix, Prefix};
use crate::data_types::{NumberLike, UnsignedLike};

const DEFAULT_COMPRESSION_LEVEL: usize = 6;
const MIN_N_TO_USE_RUN_LEN: usize = 1001;
const MIN_FREQUENCY_TO_USE_RUN_LEN: f64 = 0.8;
const DEFAULT_CHUNK_SIZE: usize = 1000000;

struct JumpstartConfiguration {
  weight: u64,
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
  /// * 0 indicates no delta encoding, so the numbers would be compressed
  /// as-is. This is perfect for columnar data were the order is essentially
  /// random.
  /// * 1st order delta encoding would take consecutive differences, leaving
  /// `[0, 2, 0, 2, 0, 2, 0]`. This is perfect for time series data like stock
  /// prices that are continuous but not smooth.
  /// * 2nd order delta encoding would take consecutive differences again,
  /// leaving `[2, -2, 2, -2, 2, -2]`. Higher-order delta encoding is good
  /// for time series like sensor readings that are very smooth.
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
  count: u64,
  n: u64,
) -> JumpstartConfiguration {
  let freq = (count as f64) / (n as f64);
  let non_freq = 1.0 - freq;
  let jumpstart = min((-non_freq.log2()).ceil() as usize, MAX_JUMPSTART);
  let expected_n_runs = (freq * non_freq * n as f64).ceil() as u64;
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
  max_n_prefix: usize,
  n: usize,
  sorted: &[T],
) {
  let count = j - i;
  let frequency = count as f64 / n as f64;
  let new_prefix_idx = max(*prefix_idx + 1, (j * max_n_prefix) / n);
  let prefix_idx_incr = new_prefix_idx - *prefix_idx;
  if n < MIN_N_TO_USE_RUN_LEN || frequency < MIN_FREQUENCY_TO_USE_RUN_LEN || count == n || prefix_idx_incr == 1 {
    // The usual case - a prefix for a range that represents either 100% or
    // <=80% of the data.
    seq.push(WeightedPrefix::new(
      count,
      count as u64,
      sorted[i],
      sorted[j - 1],
      None
    ));
  } else {
    // The weird case - a range that represents almost all (but not all) the data.
    // We create extra prefixes that can describe `reps` copies of the range at once.
    let config = choose_run_len_jumpstart(count as u64, n as u64);
    seq.push(WeightedPrefix::new(
      count,
      config.weight,
      sorted[i],
      sorted[j - 1],
      Some(config.jumpstart)
    ));
  }
  *prefix_idx = new_prefix_idx;
}

fn train_prefixes<T: NumberLike>(
  nums: Vec<T>,
  internal_config: &InternalCompressorConfig,
  flags: &Flags,
) -> QCompressResult<Vec<Prefix<T>>> {
  if nums.is_empty() {
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
  let n = nums.len();
  if n > MAX_ENTRIES {
    return Err(QCompressError::invalid_argument(format!(
      "count may not exceed {} per chunk (was {})",
      MAX_ENTRIES,
      n,
    )));
  }

  let mut sorted = nums;
  sorted.sort_unstable_by(|a, b| a.num_cmp(b));
  let safe_comp_level = min(comp_level, (n as f64).log2() as usize);
  let n_prefix = 1_usize << safe_comp_level;
  let mut prefix_sequence: Vec<WeightedPrefix<T>> = Vec::new();
  let seq_ptr = &mut prefix_sequence;

  let mut prefix_idx = 0_usize;
  let prefix_idx_ptr = &mut prefix_idx;

  let mut i = 0;
  let mut backup_j = 0_usize;
  for j in 0..n {
    let target_j = ((*prefix_idx_ptr + 1) * n) / n_prefix;
    if j > 0 && sorted[j].num_eq(&sorted[j - 1]) {
      if j >= target_j && j - target_j >= target_j - backup_j && backup_j > i {
        push_pref(seq_ptr, prefix_idx_ptr, i, backup_j, n_prefix, n, &sorted);
        i = backup_j;
      }
    } else {
      backup_j = j;
      if j >= target_j {
        push_pref(seq_ptr, prefix_idx_ptr, i, j, n_prefix, n, &sorted);
        i = j;
      }
    }
  }
  push_pref(seq_ptr, prefix_idx_ptr, i, n, n_prefix, n, &sorted);

  let mut can_improve = true;
  while can_improve {
    can_improve = false;
    let mut best_i = -1_i32;
    let mut best_improvement = 0.0;
    for i in 0..(prefix_sequence.len() - 1) {
      let pref0 = &prefix_sequence[i];
      let pref1 = &prefix_sequence[i + 1];

      let improvement = combine_improvement(pref0, pref1, n, flags);
      if improvement > best_improvement {
        can_improve = true;
        best_i = i as i32;
        best_improvement = improvement;
      }
    }

    if can_improve {
      let pref0 = &prefix_sequence[best_i as usize];
      let pref1 = &prefix_sequence[best_i as usize + 1];
      prefix_sequence[best_i as usize] = WeightedPrefix::new(
        pref0.prefix.count + pref1.prefix.count,
        pref0.weight + pref1.weight,
        pref0.prefix.lower,
        pref1.prefix.upper,
        None,
      );
      //not the most efficient but whatever
      prefix_sequence.remove(best_i as usize + 1);
    }
  }

  huffman_encoding::make_huffman_code(&mut prefix_sequence);

  let prefixes = prefix_sequence.iter()
    .map(|wp| wp.prefix.clone())
    .collect();
  Ok(prefixes)
}

fn combine_improvement<T: NumberLike>(
  wp0: &WeightedPrefix<T>,
  wp1: &WeightedPrefix<T>,
  n: usize,
  flags: &Flags,
) -> f64 {
  let p0 = &wp0.prefix;
  let p1 = &wp1.prefix;
  if p0.run_len_jumpstart.is_some() || p1.run_len_jumpstart.is_some() {
    // can never combine prefixes that encode run length
    return f64::MIN;
  }

  let p0_r_cost = avg_base2_bits(p0.upper.to_unsigned() - p0.lower.to_unsigned());
  let p1_r_cost = avg_base2_bits(p1.upper.to_unsigned() - p1.lower.to_unsigned());
  let combined_r_cost = avg_base2_bits(p1.upper.to_unsigned() - p0.lower.to_unsigned());
  let p0_d_cost = depth_bits(wp0.weight, n);
  let p1_d_cost = depth_bits(wp1.weight, n);
  let combined_d_cost = depth_bits(wp0.weight + wp1.weight, n);
  let meta_cost = 10.0 +
    flags.bits_to_encode_prefix_len() as f64 +
    2.0 * T::PHYSICAL_BITS as f64;

  let separate_cost = 2.0 * meta_cost +
    (p0_r_cost + p0_d_cost) * wp0.weight as f64+
    (p1_r_cost + p1_d_cost) * wp1.weight as f64;
  let combined_cost = meta_cost +
    (combined_r_cost + combined_d_cost) * (wp0.weight + wp1.weight) as f64;

  separate_cost - combined_cost
}


#[derive(Clone, Default)]
struct TrainedChunkCompressor<T> where T: NumberLike {
  pub prefix_infos: Vec<PrefixCompressionInfo<T>>,
}

impl<T> TrainedChunkCompressor<T> where T: NumberLike {
  pub fn new(prefixes: &[Prefix<T>]) -> QCompressResult<Self> {
    let mut prefix_infos = Vec::new();
    for p in prefixes {
      prefix_infos.push(PrefixCompressionInfo::from(p));
    }
    Ok(Self { prefix_infos })
  }

  fn compress_num_offset_bits_w_prefix(&self, num: T, pref: &PrefixCompressionInfo<T>, writer: &mut BitWriter) {
    let off = num.to_unsigned() - pref.lower_unsigned;
    writer.write_diff(off, pref.k);
    if off < pref.only_k_bits_lower || off > pref.only_k_bits_upper {
      // most significant bit, if necessary, comes last
      writer.write_one((off & (T::Unsigned::ONE << pref.k)) > T::Unsigned::ZERO);
    }
  }

  fn in_prefix(num: T, prefix: &PrefixCompressionInfo<T>) -> bool {
    num.ge(&prefix.lower) && num.le(&prefix.upper)
  }

  fn compress_nums(&self, nums: &[T], writer: &mut BitWriter) -> QCompressResult<()> {
    let mut sorted_prefixes = self.prefix_infos.clone();
    // most common prefixes come first
    sorted_prefixes.sort_by(
      |p0, p1|
        p0.count.cmp(&p1.count)
    );

    let mut i = 0;
    while i < nums.len() {
      let mut success = false;
      let num = nums[i];
      for pref in &sorted_prefixes {
        if !Self::in_prefix(num, pref) {
          continue;
        }

        writer.write(&pref.code);

        match pref.run_len_jumpstart {
          None => {
            self.compress_num_offset_bits_w_prefix(num, pref, writer);
            i += 1;
          }
          Some(jumpstart) => {
            let mut reps = 1;
            for other_num in nums.iter().skip(i + 1) {
              if Self::in_prefix(*other_num, pref) {
                reps += 1;
              } else {
                break;
              }
            }

            // we store 1 less than the number of occurrences
            // because the prefix already implies there is at least 1 occurrence
            writer.write_varint(reps - 1, jumpstart);

            for x in nums.iter().skip(i).take(reps) {
              self.compress_num_offset_bits_w_prefix(*x, pref, writer);
            }
            i += reps;
          }
        }

        success = true;
        break;
      }

      if !success {
        return Err(QCompressError::invalid_argument(format!(
          "chunk compressor's ranges were not trained to include number {}",
          nums[i],
        )));
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
    let pre_header_idx = writer.byte_size();

    let order = self.flags.delta_encoding_order;
    let (mut metadata, post_header_idx) = if order == 0 {
      let prefixes = train_prefixes(nums.to_vec(), &self.internal_config, &self.flags)?;
      let prefix_metadata = PrefixMetadata::Simple {
        prefixes: prefixes.clone(),
      };
      let metadata = ChunkMetadata {
        n,
        compressed_body_size: 0,
        prefix_metadata,
      };
      metadata.write_to(writer, &self.flags);
      let post_header_idx = writer.byte_size();
      let chunk_compressor = TrainedChunkCompressor::new(&prefixes)?;
      chunk_compressor.compress_nums(nums, writer)?;
      (metadata, post_header_idx)
    } else {
      let delta_moments = DeltaMoments::from(nums, order);
      let deltas = delta_encoding::nth_order_deltas(nums, order);
      let prefixes = train_prefixes(
        deltas.clone(),
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
      let post_header_idx = writer.byte_size();
      let chunk_compressor = TrainedChunkCompressor::new(&prefixes)?;
      chunk_compressor.compress_nums(&deltas, writer)?;
      (metadata, post_header_idx)
    };
    metadata.compressed_body_size = writer.byte_size() - post_header_idx;
    metadata.update_write_compressed_body_size(writer, pre_header_idx);
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
