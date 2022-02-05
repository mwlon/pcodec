use std::cmp::{max, min};
use std::fmt;
use std::fmt::Debug;
use std::marker::PhantomData;

use crate::{BitWriter, Flags, huffman_encoding, prefix};
use crate::bits::*;
use crate::chunk_metadata::{ChunkMetadata, PrefixInfo};
use crate::constants::*;
use crate::delta_encoding;
use crate::delta_encoding::DeltaMoments;
use crate::errors::{QCompressError, QCompressResult};
use crate::prefix::{Prefix, PrefixIntermediate};
use crate::types::{NumberLike, UnsignedLike};

const DEFAULT_COMPRESSION_LEVEL: u32 = 6;
const MIN_N_TO_USE_RUN_LEN: usize = 1001;
const MIN_FREQUENCY_TO_USE_RUN_LEN: f64 = 0.8;

struct JumpstartConfiguration {
  weight: u64,
  jumpstart: usize,
}

// everything the user might want to specify about how to compress
#[derive(Clone, Debug)]
pub struct CompressorConfig {
  pub compression_level: u32,
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
  pub compression_level: u32,
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
  seq: &mut Vec<PrefixIntermediate<T>>,
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
    seq.push(PrefixIntermediate::new(
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
    seq.push(PrefixIntermediate::new(
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
  if n as u64 > MAX_ENTRIES {
    return Err(QCompressError::invalid_argument(format!(
      "count may not exceed {} per chunk (was {})",
      MAX_ENTRIES,
      n,
    )));
  }

  let mut sorted = nums;
  sorted.sort_unstable_by(|a, b| a.num_cmp(b));
  let safe_comp_level = min(comp_level, (n as f64).log2() as u32);
  let n_prefix = 1_usize << safe_comp_level;
  let mut prefix_sequence: Vec<PrefixIntermediate<T>> = Vec::new();
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
      prefix_sequence[best_i as usize] = PrefixIntermediate::new(
        pref0.count + pref1.count,
        pref0.weight + pref1.weight,
        pref0.lower,
        pref1.upper,
        None,
      );
      //not the most efficient but whatever
      prefix_sequence.remove(best_i as usize + 1);
    }
  }

  huffman_encoding::make_huffman_code(&mut prefix_sequence);

  let mut prefixes = Vec::new();
  for p in prefix_sequence {
    prefixes.push(Prefix::from(p));
  }
  Ok(prefixes)
}

fn combine_improvement<T: NumberLike>(
  p0: &PrefixIntermediate<T>,
  p1: &PrefixIntermediate<T>,
  n: usize,
  flags: &Flags,
) -> f64 {
  if p0.run_len_jumpstart.is_some() || p1.run_len_jumpstart.is_some() {
    // can never combine prefixes that encode run length
    return f64::MIN;
  }

  let p0_r_cost = avg_base2_bits(p0.upper.to_unsigned() - p0.lower.to_unsigned());
  let p1_r_cost = avg_base2_bits(p1.upper.to_unsigned() - p1.lower.to_unsigned());
  let combined_r_cost = avg_base2_bits(p1.upper.to_unsigned() - p0.lower.to_unsigned());
  let p0_d_cost = depth_bits(p0.weight, n);
  let p1_d_cost = depth_bits(p1.weight, n);
  let combined_d_cost = depth_bits(p0.weight + p1.weight, n);
  let meta_cost = 10.0 +
    flags.bits_to_encode_prefix_len() as f64 +
    2.0 * T::PHYSICAL_BITS as f64;

  let separate_cost = 2.0 * meta_cost +
    (p0_r_cost + p0_d_cost) * p0.weight as f64+
    (p1_r_cost + p1_d_cost) * p1.weight as f64;
  let combined_cost = meta_cost +
    (combined_r_cost + combined_d_cost) * (p0.weight + p1.weight) as f64;

  separate_cost - combined_cost
}


#[derive(Clone, Default)]
struct TrainedChunkCompressor<T> where T: NumberLike {
  pub prefixes: Vec<Prefix<T>>,
}

impl<T> TrainedChunkCompressor<T> where T: NumberLike + 'static {
  fn compress_num_offset_bits_w_prefix(&self, num: T, pref: &Prefix<T>, writer: &mut BitWriter) {
    let off = num.to_unsigned() - pref.lower_unsigned;
    writer.write_diff(off, pref.k);
    if off < pref.only_k_bits_lower || off > pref.only_k_bits_upper {
      // most significant bit, if necessary, comes last
      writer.write_one((off & (T::Unsigned::ONE << pref.k)) > T::Unsigned::ZERO);
    }
  }

  fn in_prefix(num: T, prefix: &Prefix<T>) -> bool {
    num.ge(&prefix.lower) && num.le(&prefix.upper)
  }

  fn compress_nums(&self, nums: &[T], writer: &mut BitWriter) -> QCompressResult<()> {
    let mut sorted_prefixes = self.prefixes.clone();
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

        writer.write_bits(&pref.val);

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

impl<T> Debug for TrainedChunkCompressor<T> where T: NumberLike {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    prefix::display_prefixes(&self.prefixes, f)
  }
}

#[derive(Clone, Debug, Default)]
pub struct Compressor<T> where T: NumberLike {
  internal_config: InternalCompressorConfig,
  flags: Flags,
  phantom: PhantomData<T>,
}

impl<T> Compressor<T> where T: NumberLike + 'static {
  pub fn from_config(config: CompressorConfig) -> Self {
    Self {
      internal_config: InternalCompressorConfig::from(&config),
      flags: Flags::from(&config),
      ..Default::default()
    }
  }

  pub fn flags(&self) -> &Flags {
    &self.flags
  }

  pub fn header(&self, writer: &mut BitWriter) -> QCompressResult<()> {
    writer.write_aligned_bytes(&MAGIC_HEADER)?;
    writer.write_aligned_byte(T::HEADER_BYTE)?;
    self.flags.write(writer)
  }

  pub fn compress_chunk(&self, nums: &[T], writer: &mut BitWriter) -> QCompressResult<ChunkMetadata<T>> {
    if nums.is_empty() {
      return Err(QCompressError::invalid_argument(
        "cannot compress empty chunk"
      ));
    }

    writer.write_aligned_byte(MAGIC_CHUNK_BYTE)?;

    let n = nums.len();
    let pre_header_idx = writer.size();

    let order = self.flags.delta_encoding_order;
    let (mut metadata, post_header_idx) = if order == 0 {
      let prefixes = train_prefixes(nums.to_vec(), &self.internal_config, &self.flags)?;
      let prefix_info = PrefixInfo::Simple {
        prefixes: prefixes.clone(),
      };
      let metadata = ChunkMetadata {
        n,
        compressed_body_size: 0,
        prefix_info,
      };
      metadata.write_to(writer, &self.flags);
      let post_header_idx = writer.size();
      let chunk_compressor = TrainedChunkCompressor { prefixes };
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
      let prefix_info = PrefixInfo::Delta {
        delta_moments,
        prefixes: prefixes.clone(),
      };
      let metadata = ChunkMetadata {
        n,
        compressed_body_size: 0,
        prefix_info
      };
      metadata.write_to(writer, &self.flags);
      let post_header_idx = writer.size();
      let chunk_compressor = TrainedChunkCompressor { prefixes };
      chunk_compressor.compress_nums(&deltas, writer)?;
      (metadata, post_header_idx)
    };
    metadata.compressed_body_size = writer.size() - post_header_idx;
    metadata.update_write_compressed_body_size(writer, pre_header_idx);
    Ok(metadata)
  }

  pub fn footer(&self, writer: &mut BitWriter) -> QCompressResult<()> {
    writer.write_aligned_byte(MAGIC_TERMINATION_BYTE)
  }

  pub fn simple_compress(&self, nums: &[T]) -> QCompressResult<Vec<u8>> {
    let mut writer = BitWriter::default();
    self.header(&mut writer)?;
    if !nums.is_empty() {
      self.compress_chunk(nums, &mut writer)?;
    }
    self.footer(&mut writer)?;
    Ok(writer.pop())
  }
}
