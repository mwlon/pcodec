use std::cmp::{max, min};
use std::fmt::Debug;

use crate::bin::{Bin, BinCompressionInfo};
use crate::bit_writer::BitWriter;
use crate::chunk_metadata::{ChunkLatentMetadata, ChunkMetadata, PageLatentMetadata, PageMetadata};
use crate::chunk_spec::ChunkSpec;
use crate::compression_table::CompressionTable;
use crate::constants::*;
use crate::data_types::{NumberLike, UnsignedLike};
use crate::delta::DeltaMoments;
use crate::errors::{PcoError, PcoResult};
use crate::float_mult_utils::FloatMultConfig;
use crate::modes::classic::ClassicMode;
use crate::modes::gcd::{use_gcd_arithmetic, GcdMode};
use crate::modes::{gcd, Mode};
use crate::unsigned_src_dst::{DissectedLatents, DissectedSrc, LatentSrc};
use crate::{ans, delta};
use crate::{auto, Flags};
use crate::{bin_optimization, float_mult_utils};
use crate::latent_batch_dissector::LatentBatchDissector;

/// All configurations available for a compressor.
///
/// Some, like `delta_encoding_order`, are explicitly stored as `Flags` in the
/// compressed bytes.
/// Others, like `compression_level`, affect compression but are not explicitly
/// stored in the output.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct CompressorConfig {
  /// `compression_level` ranges from 0 to 12 inclusive (default 8).
  ///
  /// The compressor uses up to 2^`compression_level` bins.
  ///
  /// For example,
  /// * Level 0 achieves a small amount of compression with 1 bin.
  /// * Level 8 achieves nearly the best compression with 256 bins and still
  /// runs in reasonable time. In some cases, its compression ratio is 3-4x as
  /// high as level level 0's.
  /// * Level 12 can achieve a few % better compression than 8 with 4096
  /// bins but runs ~5x slower in many cases.
  pub compression_level: usize,
  /// `delta_encoding_order` ranges from 0 to 7 inclusive (defaults to
  /// automatically detecting on each chunk).
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
  ///
  /// If you would like to automatically choose this once and reuse it for all
  /// chunks,
  /// [`auto_compressor_config()`][crate::auto_delta_encoding_order] can help.
  pub delta_encoding_order: Option<usize>,
  /// `use_gcds` improves compression ratio in cases where all
  /// numbers in a bin share a nontrivial Greatest Common Divisor
  /// (default true).
  ///
  /// Examples where this helps:
  /// * nanosecond-precision timestamps that are all whole numbers of
  /// microseconds
  /// * integers `[7, 107, 207, 307, ... 100007]` shuffled
  ///
  /// When this is helpful, compression and decompression speeds are slightly
  /// reduced (up to ~15%). In rare cases, this configuration may reduce
  /// compression speed even when it isn't helpful.
  pub use_gcds: bool,
  /// `use_float_mult` improves compression ratio in cases where the data type
  /// is a float and all numbers are close to a multiple of a single float
  /// `base`.
  /// (default true).
  ///
  /// `base` is automatically detected. For example, this is helpful if all
  /// floats are approximately decimals (multiples of 0.01).
  ///
  /// When this is helpful, compression and decompression speeds are
  /// substantially reduced (up to ~50%). In rare cases, this configuration
  /// may reduce compression speed somewhat even when it isn't helpful.
  /// However, the compression ratio improvements tend to be quite large.
  pub use_float_mult: bool,
}

impl Default for CompressorConfig {
  fn default() -> Self {
    Self {
      compression_level: DEFAULT_COMPRESSION_LEVEL,
      delta_encoding_order: None,
      use_gcds: true,
      use_float_mult: true,
    }
  }
}

impl CompressorConfig {
  /// Sets [`compression_level`][CompressorConfig::compression_level].
  pub fn with_compression_level(mut self, level: usize) -> Self {
    self.compression_level = level;
    self
  }

  /// Sets [`delta_encoding_order`][CompressorConfig::delta_encoding_order].
  pub fn with_delta_encoding_order(mut self, order: Option<usize>) -> Self {
    self.delta_encoding_order = order;
    self
  }

  /// Sets [`use_gcds`][CompressorConfig::use_gcds].
  pub fn with_use_gcds(mut self, use_gcds: bool) -> Self {
    self.use_gcds = use_gcds;
    self
  }
}

// InternalCompressorConfig captures all settings that don't belong in flags
// i.e. these don't get written to the resulting bytes and aren't needed for
// decoding
#[derive(Clone, Debug)]
pub struct InternalCompressorConfig {
  pub compression_level: usize,
  pub delta_order: Option<usize>,
  pub use_gcds: bool,
  pub use_float_mult: bool,
}

impl InternalCompressorConfig {
  pub fn from_config<T: NumberLike>(config: &CompressorConfig) -> PcoResult<Self> {
    let compression_level = config.compression_level;
    if compression_level > MAX_COMPRESSION_LEVEL {
      return Err(PcoError::invalid_argument(format!(
        "compression level may not exceed {} (was {})",
        MAX_COMPRESSION_LEVEL, compression_level,
      )));
    }
    if let Some(order) = config.delta_encoding_order {
      if order > MAX_DELTA_ENCODING_ORDER {
        return Err(PcoError::invalid_argument(format!(
          "delta encoding order may not exceed {} (was {})",
          MAX_DELTA_ENCODING_ORDER, order,
        )));
      }
    }

    Ok(InternalCompressorConfig {
      compression_level,
      delta_order: config.delta_encoding_order,
      use_gcds: config.use_gcds,
      use_float_mult: config.use_float_mult && T::IS_FLOAT,
    })
  }
}

fn cumulative_sum(sizes: &[usize]) -> Vec<usize> {
  // there has got to be a better way to write this
  let mut res = Vec::with_capacity(sizes.len());
  let mut sum = 0;
  for s in sizes {
    res.push(sum);
    sum += s;
  }
  res
}

struct BinBuffer<'a, U: UnsignedLike> {
  pub seq: Vec<BinCompressionInfo<U>>,
  bin_idx: usize,
  max_n_bin: usize,
  n_unsigneds: usize,
  sorted: &'a [U],
  mode: Mode<U>,
  pub target_j: usize,
}

impl<'a, U: UnsignedLike> BinBuffer<'a, U> {
  fn calc_target_j(&mut self) {
    self.target_j = ((self.bin_idx + 1) * self.n_unsigneds) / self.max_n_bin
  }

  fn new(max_n_bin: usize, n_unsigneds: usize, sorted: &'a [U], mode: Mode<U>) -> Self {
    let mut res = Self {
      seq: Vec::with_capacity(max_n_bin),
      bin_idx: 0,
      max_n_bin,
      n_unsigneds,
      sorted,
      mode,
      target_j: 0,
    };
    res.calc_target_j();
    res
  }

  fn push_bin(&mut self, i: usize, j: usize) {
    let sorted = self.sorted;
    let n_unsigneds = self.n_unsigneds;

    let count = j - i;
    let new_bin_idx = max(
      self.bin_idx + 1,
      (j * self.max_n_bin) / n_unsigneds,
    );
    let lower = sorted[i];
    let upper = sorted[j - 1];

    let mut bin_gcd = U::ONE;
    if self.mode == Mode::Gcd {
      bin_gcd = gcd::gcd(&sorted[i..j]);
    }

    let bin = BinCompressionInfo {
      weight: count as Weight,
      lower,
      upper,
      gcd: bin_gcd,
      ..Default::default()
    };
    self.seq.push(bin);
    self.bin_idx = new_bin_idx;
    self.calc_target_j();
  }
}

// 2 ^ comp level, with 2 caveats:
// * Enforce n_bins <= n_unsigneds
// * Due to bin optimization compute cost ~ O(4 ^ comp level), limit max comp level when
// n_unsigneds is small
fn choose_max_n_bins(comp_level: usize, n_unsigneds: usize) -> usize {
  let log_n = (n_unsigneds as f64).log2().floor() as usize;
  let fast_comp_level = log_n.saturating_sub(4);
  let real_comp_level = if comp_level <= fast_comp_level {
    comp_level
  } else {
    fast_comp_level + comp_level.saturating_sub(fast_comp_level) / 2
  };
  min(1_usize << real_comp_level, n_unsigneds)
}

fn choose_unoptimized_mode_and_bins<U: UnsignedLike>(
  sorted: &[U],
  comp_level: usize,
  naive_mode: Mode<U>,
) -> (Mode<U>, Vec<BinCompressionInfo<U>>) {
  let n_unsigneds = sorted.len();
  let max_n_bin = choose_max_n_bins(comp_level, n_unsigneds);

  let mut i = 0;
  let mut backup_j = 0_usize;
  let mut bin_buffer = BinBuffer::<U>::new(max_n_bin, n_unsigneds, sorted, naive_mode);

  for j in 1..n_unsigneds {
    let target_j = bin_buffer.target_j;
    if sorted[j] == sorted[j - 1] {
      if j >= target_j && j - target_j >= target_j - backup_j && backup_j > i {
        bin_buffer.push_bin(i, backup_j);
        i = backup_j;
      }
    } else {
      backup_j = j;
      if j >= target_j {
        bin_buffer.push_bin(i, j);
        i = j;
      }
    }
  }
  bin_buffer.push_bin(i, n_unsigneds);

  // in some cases, we can now reduce to a simpler mode
  let unoptimized_mode = match bin_buffer.mode {
    Mode::Gcd if !gcd::use_gcd_bin_optimize(&bin_buffer.seq) => Mode::Classic,
    other => other,
  };

  (unoptimized_mode, bin_buffer.seq)
}

// returns table size log
fn quantize_weights<U: UnsignedLike>(
  infos: &mut [BinCompressionInfo<U>],
  n_unsigneds: usize,
  comp_level: usize,
) -> Bitlen {
  let counts = infos.iter().map(|info| info.weight).collect::<Vec<_>>();
  // This max size isn't big enough for all the bins when compression level is
  // high, but it gets overridden later by min compression level if necessary.
  // Going past 2^10 is undesirable because things might stop fitting in L1
  // cache.
  let max_size_log = min(comp_level as Bitlen + 2, 10);
  let (size_log, weights) = ans::quantize_weights(counts, n_unsigneds, max_size_log);
  for (i, weight) in weights.into_iter().enumerate() {
    infos[i].weight = weight;
  }
  size_log
}

#[derive(Default)]
struct TrainedBins<U: UnsignedLike> {
  infos: Vec<BinCompressionInfo<U>>,
  ans_size_log: Bitlen,
}

fn train_mode_and_infos<U: UnsignedLike>(
  unsigneds: Vec<U>,
  comp_level: usize,
  naive_mode: Mode<U>,
  n: usize, // can be greater than unsigneds.len() if delta encoding is on
) -> PcoResult<TrainedBins<U>> {
  if unsigneds.is_empty() {
    return Ok(TrainedBins::default());
  }

  if n > MAX_ENTRIES {
    return Err(PcoError::invalid_argument(format!(
      "count may not exceed {} per chunk (was {})",
      MAX_ENTRIES, n,
    )));
  }

  let n_unsigneds = unsigneds.len();
  let (unoptimized_mode, unoptimized_bins) = {
    let mut sorted = unsigneds;
    sorted.sort_unstable();
    choose_unoptimized_mode_and_bins(&sorted, comp_level, naive_mode)
  };

  let estimated_ans_size_log = (comp_level + 2) as Bitlen;
  let mut optimized_infos = match unoptimized_mode {
    Mode::Classic | Mode::FloatMult { .. } => bin_optimization::optimize_bins(
      unoptimized_bins,
      estimated_ans_size_log,
      ClassicMode,
      n,
    ),
    Mode::Gcd => bin_optimization::optimize_bins(
      unoptimized_bins,
      estimated_ans_size_log,
      GcdMode,
      n,
    ),
  };

  let ans_size_log = quantize_weights(&mut optimized_infos, n_unsigneds, comp_level);

  Ok(TrainedBins {
    infos: optimized_infos,
    ans_size_log,
  })
}

fn empty_vec<T>(n: usize) -> Vec<T> {
  unsafe {
    let mut res = Vec::with_capacity(n);
    res.set_len(n);
    res
  }
}

fn dissect_unsigneds<U: UnsignedLike>(
  mid_chunk_info: &MidChunkInfo<U>,
) -> PcoResult<DissectedSrc<U>> {
  let MidChunkInfo {
    latent_configs,
    src,
    needs_gcds,
    n_nontrivial_latents,
    ..
  } = mid_chunk_info;

  let uninit_dissected_latents = |n, ans_default_state| {
    let ans_final_states = [ans_default_state; ANS_INTERLEAVING];
    DissectedLatents {
      ans_vals: empty_vec(n),
      ans_bits: empty_vec(n),
      offsets: empty_vec(n),
      offset_bits: empty_vec(n),
      ans_final_states,
    }
  };

  let mut res = DissectedSrc {
    page_n: src.page_n,
    dissected_latents: Vec::new(),
  };

  for (latent_idx, config) in latent_configs.iter().take(*n_nontrivial_latents).enumerate() {
    let latents = &src.latents[latent_idx];
    let LatentConfig { table, encoder, .. } = config;
    let mut dissected_latents = uninit_dissected_latents(latents.len(), encoder.default_state());

    // we go through in reverse for ANS!
    let mut lbd = LatentBatchDissector::new(*needs_gcds, table, encoder);
    for (batch_idx, batch) in latents.chunks(FULL_BATCH_SIZE).enumerate().rev() {
      let base_i = batch_idx * FULL_BATCH_SIZE;
      lbd.analyze_latent_batch(
        batch,
        base_i,
        &mut dissected_latents,
      )
    }
    res.dissected_latents.push(dissected_latents);
  }

  Ok(res)
}

fn write_dissecteds<U: UnsignedLike>(
  src: DissectedSrc<U>,
  writer: &mut BitWriter,
) -> PcoResult<()> {
  // TODO make this more SIMD like LatentBatchDecompressor::unchecked_decompress_offsets
  let mut batch_start = 0;
  while batch_start < src.page_n {
    let batch_end = min(batch_start + FULL_BATCH_SIZE, src.page_n);
    for dissected in &src.dissected_latents {
      let latent_batch_end = min(batch_end, dissected.ans_vals.len());
      assert!(dissected.ans_bits.len() >= latent_batch_end);
      assert!(dissected.offsets.len() >= latent_batch_end);
      assert!(dissected.offset_bits.len() >= latent_batch_end);
      for i in batch_start..latent_batch_end {
        writer.write_diff(
          dissected.ans_vals[i],
          dissected.ans_bits[i],
        );
      }
      for i in batch_start..latent_batch_end {
        writer.write_diff(
          dissected.offsets[i],
          dissected.offset_bits[i],
        );
      }
    }
    batch_start = batch_end;
  }

  writer.finish_byte();
  Ok(())
}

#[derive(Clone, Debug)]
pub struct MidChunkInfo<U: UnsignedLike> {
  // immutable:
  latent_configs: Vec<LatentConfig<U>>,
  page_sizes: Vec<usize>,
  n_latents: usize,
  n_nontrivial_latents: usize,
  needs_gcds: bool,

  // mutable:
  src: LatentSrc<U>,
  page_idx: usize,
}

impl<U: UnsignedLike> MidChunkInfo<U> {
  fn page_moments(&self, latent_idx: usize) -> &DeltaMoments<U> {
    &self.latent_configs[latent_idx].delta_momentss[self.page_idx]
  }

  fn n_pages(&self) -> usize {
    self.page_sizes.len()
  }
}

#[derive(Clone, Debug, Default)]
pub enum State<U: UnsignedLike> {
  #[default]
  PreHeader,
  StartOfChunk,
  MidChunk(MidChunkInfo<U>),
  Terminated,
}

impl<U: UnsignedLike> State<U> {
  pub fn wrong_step_err(&self, description: &str) -> PcoError {
    let step_str = match self {
      State::PreHeader => "has not yet written header",
      State::StartOfChunk => "is at the start of a chunk",
      State::MidChunk(_) => "is mid-chunk",
      State::Terminated => "has already written the footer",
    };
    PcoError::invalid_argument(format!(
      "attempted to write {} when compressor {}",
      description, step_str,
    ))
  }
}

#[derive(Clone, Debug)]
struct LatentConfig<U: UnsignedLike> {
  table: CompressionTable<U>,
  encoder: ans::Encoder,
  delta_momentss: Vec<DeltaMoments<U>>,
}

#[derive(Clone, Debug)]
pub struct BaseCompressor<T: NumberLike> {
  internal_config: InternalCompressorConfig,
  pub flags: Flags,
  pub writer: BitWriter,
  pub state: State<T::Unsigned>,
}

fn bins_from_compression_infos<U: UnsignedLike>(infos: &[BinCompressionInfo<U>]) -> Vec<Bin<U>> {
  infos.iter().cloned().map(Bin::from).collect()
}

impl<T: NumberLike> BaseCompressor<T> {
  pub fn from_config(config: CompressorConfig, use_wrapped_mode: bool) -> PcoResult<Self> {
    Ok(Self {
      internal_config: InternalCompressorConfig::from_config::<T>(&config)?,
      flags: Flags::from_config(&config, use_wrapped_mode),
      writer: BitWriter::default(),
      state: State::default(),
    })
  }

  pub fn header(&mut self) -> PcoResult<()> {
    if !matches!(self.state, State::PreHeader) {
      return Err(self.state.wrong_step_err("header"));
    }

    self.writer.write_aligned_bytes(&MAGIC_HEADER)?;
    self.writer.write_aligned_byte(T::HEADER_BYTE)?;
    self.flags.write_to(&mut self.writer)?;
    self.state = State::StartOfChunk;
    Ok(())
  }

  fn choose_naive_mode(&self, nums: &[T]) -> Mode<T::Unsigned> {
    // * Use float mult if enabled and an appropriate base is found
    // * Otherwise, use GCD if enabled
    // * Otherwise, use Classic
    if self.internal_config.use_float_mult {
      if let Some(config) = float_mult_utils::choose_config::<T>(nums) {
        return Mode::FloatMult(config);
      }
    }

    if self.internal_config.use_gcds {
      Mode::Gcd
    } else {
      Mode::Classic
    }
  }

  fn split_latents(&self, naive_mode: Mode<T::Unsigned>, nums: &[T]) -> LatentSrc<T::Unsigned> {
    match naive_mode {
      Mode::Classic | Mode::Gcd => LatentSrc::new(
        nums.len(),
        vec![nums.iter().map(|x| x.to_unsigned()).collect()],
      ),
      Mode::FloatMult(FloatMultConfig { base, inv_base }) => {
        float_mult_utils::split_latents(nums, base, inv_base)
      }
    }
  }

  // This function actually does much of the work of compressing the whole
  // chunk. We defer as much work as we can to writing the data pages though.
  pub fn chunk_metadata_internal(
    &mut self,
    nums: &[T],
    spec: &ChunkSpec,
  ) -> PcoResult<ChunkMetadata<T::Unsigned>> {
    if !matches!(self.state, State::StartOfChunk) {
      return Err(self.state.wrong_step_err("chunk metadata"));
    }

    if nums.is_empty() {
      return Err(PcoError::invalid_argument(
        "cannot compress empty chunk",
      ));
    }

    let n = nums.len();
    let page_sizes = spec.page_sizes(nums.len())?;

    if !self.flags.use_wrapped_mode {
      self.writer.write_aligned_byte(MAGIC_CHUNK_BYTE)?;
    }

    let naive_mode = self.choose_naive_mode(nums);
    let mut src = self.split_latents(naive_mode, nums);
    let page_idxs = cumulative_sum(&page_sizes);
    let n_latents = naive_mode.n_latents();

    let delta_order = if let Some(delta_order) = self.internal_config.delta_order {
      delta_order
    } else {
      auto::auto_delta_encoding_order(nums, self.internal_config.compression_level)
    };

    let mut latent_metas = Vec::with_capacity(n_latents);
    let mut latent_configs = Vec::with_capacity(n_latents);
    for latent_idx in 0..n_latents {
      let delta_order = naive_mode.latent_delta_order(latent_idx, delta_order);
      let delta_momentss = delta::encode_in_place(
        &mut src.latents[latent_idx],
        delta_order,
        &page_idxs,
      );

      // secondary latents should be compressed faster
      let comp_level = if latent_idx == 0 {
        self.internal_config.compression_level
      } else {
        min(self.internal_config.compression_level, 5)
      };

      let trained = train_mode_and_infos(
        src.latents[latent_idx].to_vec(),
        comp_level,
        naive_mode,
        n,
      )?;
      let bins = bins_from_compression_infos(&trained.infos);

      let table = CompressionTable::from(trained.infos);
      let encoder = ans::Encoder::from_bins(trained.ans_size_log, &bins)?;

      latent_metas.push(ChunkLatentMetadata {
        bins,
        ans_size_log: trained.ans_size_log,
      });
      latent_configs.push(LatentConfig {
        table,
        delta_momentss,
        encoder,
      });
    }

    let optimized_mode = match naive_mode {
      Mode::Gcd => {
        if latent_metas.iter().any(|m| use_gcd_arithmetic(&m.bins)) {
          Mode::Gcd
        } else {
          Mode::Classic
        }
      }
      other => other,
    };

    let meta = ChunkMetadata::new(n, optimized_mode, delta_order, latent_metas);
    meta.write_to(&self.flags, &mut self.writer);

    let n_latents = optimized_mode.n_latents();
    let (needs_gcds, n_nontrivial_latents) = meta.nontrivial_gcd_and_n_latents();

    self.state = State::MidChunk(MidChunkInfo {
      latent_configs,
      page_sizes,
      n_latents,
      n_nontrivial_latents,
      needs_gcds,
      src,
      page_idx: 0,
    });

    Ok(meta)
  }

  pub fn page_internal(&mut self) -> PcoResult<()> {
    let info = match &mut self.state {
      State::MidChunk(info) => Ok(info),
      other => Err(other.wrong_step_err("data page")),
    }?;

    let dissected_src = dissect_unsigneds(info)?;

    let mut latent_metas = Vec::with_capacity(info.n_latents);
    for latent_idx in 0..info.n_latents {
      let delta_moments = info.page_moments(latent_idx).clone();

      // write the final ANS state, moving it down the range [0, table_size)
      let ans_final_state_idxs = dissected_src
        .dissected_latents
        .get(latent_idx)
        .map(|dissected| dissected.ans_final_states)
        .unwrap_or([0; ANS_INTERLEAVING]);
      latent_metas.push(PageLatentMetadata {
        delta_moments,
        ans_final_state_idxs,
      });
    }
    let page_meta = PageMetadata {
      latents: latent_metas,
    };
    let ans_size_logs = info
      .latent_configs
      .iter()
      .map(|config| config.encoder.size_log());
    page_meta.write_to(ans_size_logs, &mut self.writer);

    write_dissecteds(dissected_src, &mut self.writer)?;

    info.page_idx += 1;

    if info.page_idx == info.n_pages() {
      self.state = State::StartOfChunk;
    }

    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::choose_max_n_bins;

  #[test]
  fn test_choose_max_n_bins() {
    assert_eq!(choose_max_n_bins(0, 100), 1);
    assert_eq!(choose_max_n_bins(12, 200), 1 << 7);
    assert_eq!(choose_max_n_bins(12, 1 << 10), 1 << 9);
    assert_eq!(choose_max_n_bins(8, 1 << 10), 1 << 7);
    assert_eq!(choose_max_n_bins(1, 1 << 10), 2);
    assert_eq!(choose_max_n_bins(12, (1 << 12) - 1), 1 << 9);
    assert_eq!(choose_max_n_bins(12, 1 << 12), 1 << 10);
    assert_eq!(choose_max_n_bins(12, (1 << 16) - 1), 1 << 11);
    assert_eq!(choose_max_n_bins(12, 1 << 16), 1 << 12);
    assert_eq!(choose_max_n_bins(12, 1 << 20), 1 << 12);
  }
}
