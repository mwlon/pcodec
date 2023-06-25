use std::cmp::{max, min};
use std::fmt::Debug;

use crate::ans::AnsEncoder;
use crate::bin::{Bin, BinCompressionInfo};
use crate::bit_writer::BitWriter;
use crate::chunk_metadata::ChunkMetadata;
use crate::chunk_spec::ChunkSpec;
use crate::compression_table::CompressionTable;
use crate::constants::*;
use crate::data_types::{NumberLike, UnsignedLike};
use crate::delta_encoding::DeltaMoments;
use crate::errors::{QCompressError, QCompressResult};
use crate::modes::adjusted::AdjustedMode;
use crate::modes::classic::ClassicMode;
use crate::modes::gcd::GcdMode;
use crate::modes::{adjusted, gcd, DynMode};
use crate::unsigned_src_dst::{DecomposedUnsigned, UnsignedSrc};
use crate::Flags;
use crate::{ans, delta_encoding};
use crate::{bin_optimization, float_mult_utils};

/// All configurations available for a compressor.
///
/// Some, like `delta_encoding_order`, are explicitly stored as `Flags` in the
/// compressed bytes.
/// Others, like `compression_level`, affect compression but are not explicitly
/// stored in the output.
#[derive(Clone, Debug)]
pub struct CompressorConfig {
  /// `compression_level` ranges from 0 to 12 inclusive (default 8).
  ///
  /// The compressor uses up to 2^`compression_level` bins.
  ///
  /// For example,
  /// * Level 0 achieves a modest amount of compression with 1 bin and can
  /// be twice as fast as level 8.
  /// * Level 8 achieves nearly the best compression with 256 bins and still
  /// runs in reasonable time. In some cases, its compression ratio is 3-4x as
  /// high as level level 0's.
  /// * Level 12 can achieve a few % better compression than 8 with 4096
  /// bins but runs ~5x slower in many cases.
  pub compression_level: usize,
  /// `delta_encoding_order` ranges from 0 to 7 inclusive (default 0).
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
  /// Setting delta encoding order too high or low will hurt compression ratio.
  /// If you're unsure, use
  /// [`auto_compressor_config()`][crate::auto_compressor_config] to choose it.
  pub delta_encoding_order: usize,
  /// `use_gcds` improves compression ratio in cases where all
  /// numbers in a range share a nontrivial Greatest Common Divisor
  /// (default true).
  ///
  /// Examples where this helps:
  /// * integers `[7, 107, 207, 307, ... 100007]` shuffled
  /// * floats `[1.0, 2.0, ... 1000.0]` shuffled
  /// * nanosecond-precision timestamps that are all whole numbers of
  /// microseconds
  ///
  /// When this is helpful and in rare cases when it isn't, compression speed
  /// is slightly reduced.
  pub use_gcds: bool,
  // TODO
  pub use_float_mult: bool,
}

impl Default for CompressorConfig {
  fn default() -> Self {
    Self {
      compression_level: DEFAULT_COMPRESSION_LEVEL,
      delta_encoding_order: 0,
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
  pub fn with_delta_encoding_order(mut self, order: usize) -> Self {
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
  pub use_gcds: bool,
  pub use_float_mult: bool,
}

impl InternalCompressorConfig {
  pub fn from_config<T: NumberLike>(config: &CompressorConfig) -> Self {
    InternalCompressorConfig {
      compression_level: config.compression_level,
      use_gcds: config.use_gcds,
      use_float_mult: config.use_float_mult && T::IS_FLOAT,
    }
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
  mode: DynMode<U>,
  pub target_j: usize,
}

impl<'a, U: UnsignedLike> BinBuffer<'a, U> {
  fn calc_target_j(&mut self) {
    self.target_j = ((self.bin_idx + 1) * self.n_unsigneds) / self.max_n_bin
  }

  fn new(max_n_bin: usize, n_unsigneds: usize, sorted: &'a [U], mode: DynMode<U>) -> Self {
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
    if self.mode == DynMode::Gcd {
      bin_gcd = gcd::gcd(&sorted[i..j]);
    }

    let bin = BinCompressionInfo {
      weight: count,
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
  internal_config: &InternalCompressorConfig,
  naive_mode: DynMode<U>,
) -> (DynMode<U>, Vec<BinCompressionInfo<U>>) {
  let n_unsigneds = sorted.len();
  let max_n_bin = choose_max_n_bins(
    internal_config.compression_level,
    n_unsigneds,
  );

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
    DynMode::Gcd if !gcd::use_gcd_bin_optimize(&bin_buffer.seq) => DynMode::Classic,
    other => other,
  };

  (unoptimized_mode, bin_buffer.seq)
}

// returns table size log
fn quantize_weights<U: UnsignedLike>(
  infos: &mut [BinCompressionInfo<U>],
  n_unsigneds: usize,
  internal_config: &InternalCompressorConfig,
) -> Bitlen {
  let counts = infos.iter().map(|info| info.weight).collect::<Vec<_>>();
  let max_size_log = internal_config.compression_level as Bitlen + 2;
  let (size_log, weights) = ans::quantize_weights(counts, n_unsigneds, max_size_log);
  for (i, weight) in weights.into_iter().enumerate() {
    infos[i].weight = weight;
  }
  size_log
}

#[derive(Default)]
struct TrainedBins<U: UnsignedLike> {
  dyn_mode: DynMode<U>,
  infos: Vec<BinCompressionInfo<U>>,
  ans_size_log: Bitlen,
}

fn train_mode_and_bins<U: UnsignedLike>(
  unsigneds: Vec<U>,
  internal_config: &InternalCompressorConfig,
  naive_mode: DynMode<U>,
  n: usize, // can be greater than unsigneds.len() if delta encoding is on
) -> QCompressResult<TrainedBins<U>> {
  if unsigneds.is_empty() {
    return Ok(TrainedBins::default());
  }

  let comp_level = internal_config.compression_level;
  if comp_level > MAX_COMPRESSION_LEVEL {
    return Err(QCompressError::invalid_argument(format!(
      "compression level may not exceed {} (was {})",
      MAX_COMPRESSION_LEVEL, comp_level,
    )));
  }
  if n > MAX_ENTRIES {
    return Err(QCompressError::invalid_argument(format!(
      "count may not exceed {} per chunk (was {})",
      MAX_ENTRIES, n,
    )));
  }

  let n_unsigneds = unsigneds.len();
  let (unoptimized_mode, unoptimized_bins) = {
    let mut sorted = unsigneds;
    sorted.sort_unstable();
    choose_unoptimized_mode_and_bins(&sorted, internal_config, naive_mode)
  };

  let estimated_ans_size_log = (internal_config.compression_level + 2) as Bitlen;
  let mut optimized_infos = match unoptimized_mode {
    DynMode::Classic => bin_optimization::optimize_bins(
      unoptimized_bins,
      estimated_ans_size_log,
      ClassicMode,
      n,
    ),
    DynMode::Gcd => bin_optimization::optimize_bins(
      unoptimized_bins,
      estimated_ans_size_log,
      GcdMode,
      n,
    ),
    DynMode::FloatMult { adj_bits, .. } => bin_optimization::optimize_bins(
      unoptimized_bins,
      estimated_ans_size_log,
      AdjustedMode::new(adj_bits),
      n,
    ),
  };

  let ans_size_log = quantize_weights(
    &mut optimized_infos,
    n_unsigneds,
    internal_config,
  );

  Ok(TrainedBins {
    dyn_mode: unoptimized_mode,
    infos: optimized_infos,
    ans_size_log,
  })
}

fn trained_compress_body<U: UnsignedLike>(
  src: &mut UnsignedSrc<U>,
  flags: &Flags,
  table: &CompressionTable<U>,
  encoder: &mut AnsEncoder,
  dyn_mode: DynMode<U>,
  writer: &mut BitWriter,
) -> QCompressResult<()> {
  let (use_gcd, adjustment_bits) = match dyn_mode {
    DynMode::Classic => (false, 0),
    DynMode::Gcd => (true, 0),
    DynMode::FloatMult { adj_bits, .. } => (false, adj_bits),
  };
  if use_gcd {
    decompose_unsigneds::<U, true>(table, encoder, src)?;
  } else {
    decompose_unsigneds::<U, false>(table, encoder, src)?;
  }
  // write the final ANS state idx in [0, table_size)
  writer.write_usize(
    encoder.state() - (1 << encoder.size_log()),
    encoder.size_log(),
  );
  writer.finish_byte();

  if adjustment_bits > 0 {
    compress_data_page::<U, true>(src, flags, adjustment_bits, writer)
  } else {
    compress_data_page::<U, false>(src, flags, adjustment_bits, writer)
  }
}

// returns the ANS final state after decomposing the unsigneds in reverse order
fn decompose_unsigneds<U: UnsignedLike, const USE_GCD: bool>(
  table: &CompressionTable<U>,
  encoder: &mut AnsEncoder,
  src: &mut UnsignedSrc<U>,
) -> QCompressResult<()> {
  let unsigneds = src.unsigneds();
  let mut decomposeds = Vec::with_capacity(unsigneds.len());
  unsafe { decomposeds.set_len(unsigneds.len()) }
  for i in (0..unsigneds.len()).rev() {
    let u = unsigneds[i];
    let info = table.search(u)?;
    let (ans_word, ans_bits) = encoder.encode(info.token);
    let offset = if USE_GCD {
      (u - info.lower) / info.gcd
    } else {
      u - info.lower
    };
    decomposeds[i] = DecomposedUnsigned {
      ans_word,
      ans_bits,
      offset,
      offset_bits: info.offset_bits,
    };
  }
  src.set_decomposeds(decomposeds);
  Ok(())
}

fn compress_data_page<U: UnsignedLike, const USE_ADJUSTMENT: bool>(
  src: &mut UnsignedSrc<U>,
  flags: &Flags,
  adj_bits: Bitlen,
  writer: &mut BitWriter,
) -> QCompressResult<()> {
  let adj_lower = adjusted::calc_adj_lower(adj_bits);
  while !src.finished_unsigneds() {
    let decomposed = src.decomposed();
    writer.write_usize(decomposed.ans_word, decomposed.ans_bits);
    writer.write_diff(decomposed.offset, decomposed.offset_bits);
    if USE_ADJUSTMENT {
      writer.write_diff(
        src.adjustment().wrapping_sub(adj_lower),
        adj_bits,
      );
    }
    src.incr();
  }

  // if delta encoding is used, we have a few fewer deltas than adjustments
  if USE_ADJUSTMENT {
    for _ in 0..flags.delta_encoding_order {
      writer.write_diff(
        src.adjustment().wrapping_sub(adj_lower),
        adj_bits,
      );
      src.incr();
    }
  }
  writer.finish_byte();
  Ok(())
}

#[derive(Clone, Debug)]
pub struct MidChunkInfo<U: UnsignedLike> {
  // immutable:
  dyn_mode: DynMode<U>,
  table: CompressionTable<U>,
  delta_momentss: Vec<DeltaMoments<U>>,
  page_sizes: Vec<usize>,
  // mutable:
  src: UnsignedSrc<U>,
  encoder: AnsEncoder,
  page_idx: usize,
}

impl<U: UnsignedLike> MidChunkInfo<U> {
  fn data_page_moments(&self) -> &DeltaMoments<U> {
    &self.delta_momentss[self.page_idx]
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
  pub fn wrong_step_err(&self, description: &str) -> QCompressError {
    let step_str = match self {
      State::PreHeader => "has not yet written header",
      State::StartOfChunk => "is at the start of a chunk",
      State::MidChunk(_) => "is mid-chunk",
      State::Terminated => "has already written the footer",
    };
    QCompressError::invalid_argument(format!(
      "attempted to write {} when compressor {}",
      description, step_str,
    ))
  }
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
  pub fn from_config(config: CompressorConfig, use_wrapped_mode: bool) -> Self {
    Self {
      internal_config: InternalCompressorConfig::from_config::<T>(&config),
      flags: Flags::from_config(&config, use_wrapped_mode),
      writer: BitWriter::default(),
      state: State::default(),
    }
  }

  pub fn header(&mut self) -> QCompressResult<()> {
    if !matches!(self.state, State::PreHeader) {
      return Err(self.state.wrong_step_err("header"));
    }

    self.writer.write_aligned_bytes(&MAGIC_HEADER)?;
    self.writer.write_aligned_byte(T::HEADER_BYTE)?;
    self.flags.write_to(&mut self.writer)?;
    self.state = State::StartOfChunk;
    Ok(())
  }

  fn choose_naive_mode(&self, nums: &[T]) -> DynMode<T::Unsigned> {
    // * Use float mult if enabled and an appropriate base is found
    // * Otherwise, use GCD if enabled
    // * Otherwise, use Classic
    if self.internal_config.use_float_mult {
      if let Some(config) = float_mult_utils::choose_config::<T>(nums) {
        return DynMode::float_mult(config);
      }
    }

    if self.internal_config.use_gcds {
      DynMode::Gcd
    } else {
      DynMode::Classic
    }
  }

  fn preprocess_src(
    &self,
    nums: &[T],
  ) -> (
    DynMode<T::Unsigned>,
    UnsignedSrc<T::Unsigned>,
  ) {
    let dyn_mode = self.choose_naive_mode(nums);
    let src = dyn_mode.create_src(nums);
    (dyn_mode, src)
  }

  pub fn chunk_metadata_internal(
    &mut self,
    nums: &[T],
    spec: &ChunkSpec,
  ) -> QCompressResult<ChunkMetadata<T::Unsigned>> {
    if !matches!(self.state, State::StartOfChunk) {
      return Err(self.state.wrong_step_err("chunk metadata"));
    }

    if nums.is_empty() {
      return Err(QCompressError::invalid_argument(
        "cannot compress empty chunk",
      ));
    }

    let n = nums.len();
    let page_sizes = spec.page_sizes(nums.len())?;

    if !self.flags.use_wrapped_mode {
      self.writer.write_aligned_byte(MAGIC_CHUNK_BYTE)?;
    }

    let order = self.flags.delta_encoding_order;
    let (naive_mode, mut src) = self.preprocess_src(nums);
    let page_idxs = cumulative_sum(&page_sizes);
    let delta_momentss = delta_encoding::nth_order_deltas(src.unsigneds_mut(), order, &page_idxs);
    let trained_bins = train_mode_and_bins(
      src.unsigneds().to_vec(),
      &self.internal_config,
      naive_mode,
      n,
    )?;
    let bins = bins_from_compression_infos(&trained_bins.infos);

    let optimized_mode = match trained_bins.dyn_mode {
      DynMode::Gcd if !gcd::use_gcd_arithmetic(&bins) => DynMode::Classic,
      other => other,
    };

    let table = CompressionTable::from(trained_bins.infos);
    let encoder = AnsEncoder::from_bins(trained_bins.ans_size_log, &bins)?;

    let meta = ChunkMetadata::new(
      n,
      bins,
      optimized_mode,
      trained_bins.ans_size_log,
    );
    meta.write_to(&self.flags, &mut self.writer);

    self.state = State::MidChunk(MidChunkInfo {
      dyn_mode: optimized_mode,
      table,
      encoder,
      delta_momentss,
      page_sizes,
      src,
      page_idx: 0,
    });

    Ok(meta)
  }

  pub fn data_page_internal(&mut self) -> QCompressResult<()> {
    let has_pages_remaining = {
      let info = match &mut self.state {
        State::MidChunk(info) => Ok(info),
        other => Err(other.wrong_step_err("data page")),
      }?;

      info.data_page_moments().write_to(&mut self.writer);
      trained_compress_body(
        &mut info.src,
        &self.flags,
        &info.table,
        &mut info.encoder,
        info.dyn_mode,
        &mut self.writer,
      )?;

      info.page_idx += 1;

      info.page_idx < info.n_pages()
    };

    if !has_pages_remaining {
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
