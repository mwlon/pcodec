use std::cmp::{max, min};
use std::io::Write;

use crate::bin::BinCompressionInfo;
use crate::bit_writer::BitWriter;
use crate::compression_table::CompressionTable;
use crate::constants::{
  Bitlen, Weight, ANS_INTERLEAVING, CHUNK_META_PADDING, MAX_COMPRESSION_LEVEL,
  MAX_DELTA_ENCODING_ORDER, MAX_ENTRIES, PAGE_PADDING,
};
use crate::data_types::{NumberLike, UnsignedLike};
use crate::delta::DeltaMoments;
use crate::errors::{PcoError, PcoResult};
use crate::float_mult_utils::FloatMultConfig;
use crate::latent_batch_dissector::LatentBatchDissector;
use crate::modes::classic::ClassicMode;
use crate::modes::gcd;
use crate::modes::gcd::{use_gcd_arithmetic, GcdMode};
use crate::page_meta::{PageLatentMeta, PageMeta};
use crate::unsigned_src_dst::{DissectedLatents, DissectedSrc, LatentSrc};
use crate::{
  ans, bin_optimization, bits, delta, float_mult_utils, Bin, ChunkConfig, ChunkLatentMeta,
  ChunkMeta, Mode, FULL_BATCH_SIZE,
};

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
  // This max size is just big enough to handle the maximum number of bins,
  // and it's small enough that the encoding/decoding ANS tables will
  // mostly fit into L1 cache. We cap it so that higher compression levels
  // don't incur substantially slower decompression.
  let max_size_log = min(comp_level as Bitlen + 2, 12);
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

fn uninit_vec<T>(n: usize) -> Vec<T> {
  unsafe {
    let mut res = Vec::with_capacity(n);
    res.set_len(n);
    res
  }
}

fn write_dissecteds<U: UnsignedLike, W: Write>(
  src: DissectedSrc<U>,
  writer: &mut BitWriter<W>,
) -> PcoResult<()> {
  // TODO make this more SIMD like LatentBatchDecompressor::unchecked_decompress_offsets
  let mut batch_start = 0;
  while batch_start < src.page_size {
    let batch_end = min(batch_start + FULL_BATCH_SIZE, src.page_size);
    for dissected in &src.dissected_latents {
      for (&val, &bits) in dissected
        .ans_vals
        .iter()
        .zip(dissected.ans_bits.iter())
        .skip(batch_start)
        .take(FULL_BATCH_SIZE)
      {
        writer.write_uint(val, bits);
      }
      for (&offset, &bits) in dissected
        .offsets
        .iter()
        .zip(dissected.offset_bits.iter())
        .skip(batch_start)
        .take(FULL_BATCH_SIZE)
      {
        writer.write_uint(offset, bits);
      }
      writer.flush()?;
    }
    batch_start = batch_end;
  }
  Ok(())
}

#[derive(Clone, Debug)]
struct LatentConfig<U: UnsignedLike> {
  table: CompressionTable<U>,
  encoder: ans::Encoder,
  delta_momentss: Vec<DeltaMoments<U>>, // one per page
}

/// Holds metadata about a chunk and supports compression.
#[derive(Clone, Debug)]
pub struct ChunkCompressor<U: UnsignedLike> {
  meta: ChunkMeta<U>,
  latent_configs: Vec<LatentConfig<U>>,
  page_sizes: Vec<usize>,
  n_latents: usize,
  n_nontrivial_latents: usize,
  needs_gcds: bool,
  src: LatentSrc<U>,
  max_bits_per_latent: Vec<Bitlen>, // one per latent var
}

fn bins_from_compression_infos<U: UnsignedLike>(infos: &[BinCompressionInfo<U>]) -> Vec<Bin<U>> {
  infos.iter().cloned().map(Bin::from).collect()
}

fn choose_naive_mode<T: NumberLike>(nums: &[T], config: &ChunkConfig) -> Mode<T::Unsigned> {
  // * Use float mult if enabled and an appropriate base is found
  // * Otherwise, use GCD if enabled
  // * Otherwise, use Classic
  if config.use_float_mult && T::IS_FLOAT {
    if let Some(config) = float_mult_utils::choose_config::<T>(nums) {
      return Mode::FloatMult(config);
    }
  }

  if config.use_gcds {
    Mode::Gcd
  } else {
    Mode::Classic
  }
}

fn split_latents<T: NumberLike>(
  naive_mode: Mode<T::Unsigned>,
  nums: &[T],
) -> LatentSrc<T::Unsigned> {
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

fn validate_config(config: &ChunkConfig) -> PcoResult<()> {
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

  Ok(())
}

fn validate_chunk_size(n: usize) -> PcoResult<()> {
  if n == 0 {
    return Err(PcoError::invalid_argument(
      "cannot compress empty chunk",
    ));
  }
  if n > MAX_ENTRIES {
    return Err(PcoError::invalid_argument(format!(
      "count may not exceed {} per chunk (was {})",
      MAX_ENTRIES, n,
    )));
  }

  Ok(())
}

pub(crate) fn new<T: NumberLike>(
  nums: &[T],
  config: &ChunkConfig,
) -> PcoResult<ChunkCompressor<T::Unsigned>> {
  validate_config(config)?;
  let n = nums.len();
  validate_chunk_size(n)?;

  let page_sizes = config.paging_spec.page_sizes(nums.len())?;

  let naive_mode = choose_naive_mode(nums, config);
  let mut src = split_latents(naive_mode, nums);
  let page_idxs = cumulative_sum(&page_sizes);
  let n_latents = naive_mode.n_latents();

  let delta_order = if let Some(delta_order) = config.delta_encoding_order {
    delta_order
  } else {
    crate::auto_delta_encoding_order(nums, config.compression_level)?
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
      config.compression_level
    } else {
      min(config.compression_level, 5)
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

    latent_metas.push(ChunkLatentMeta {
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

  let meta = ChunkMeta::new(optimized_mode, delta_order, latent_metas);
  let max_bits_per_latent = meta
    .latents
    .iter()
    .map(|latent_meta| latent_meta.max_bits_per_ans() + latent_meta.max_bits_per_offset())
    .collect::<Vec<_>>();

  let n_latents = optimized_mode.n_latents();
  let (needs_gcds, n_nontrivial_latents) = meta.nontrivial_gcd_and_n_latents();

  Ok(ChunkCompressor {
    meta,
    latent_configs,
    page_sizes,
    n_latents,
    n_nontrivial_latents,
    needs_gcds,
    src,
    max_bits_per_latent,
  })
}

impl<U: UnsignedLike> ChunkCompressor<U> {
  fn page_moments(&self, page_idx: usize, latent_idx: usize) -> &DeltaMoments<U> {
    &self.latent_configs[latent_idx].delta_momentss[page_idx]
  }

  /// Returns the count of numbers this chunk will contain in each page.
  pub fn page_sizes(&self) -> &[usize] {
    &self.page_sizes
  }

  /// Returns pre-computed information about the chunk.
  pub fn meta(&self) -> &ChunkMeta<U> {
    &self.meta
  }

  /// Returns an estimate of the overall size of the chunk.
  ///
  /// This can be useful when building the file as a `Vec<u8>` in memory;
  /// you can `.reserve()` ahead of time.
  pub fn chunk_meta_size_hint(&self) -> usize {
    let mut bytes = 32;
    let bytes_per_num = U::BITS / 8;
    for latent_meta in &self.meta.latents {
      bytes += latent_meta.bins.len() * (4 + 2 * bytes_per_num as usize)
    }
    bytes
  }

  /// Writes the chunk metadata to the destination.
  ///
  /// Will return an error if the provided `Write` errors.
  pub fn write_chunk_meta<W: Write>(&self, dst: W) -> PcoResult<W> {
    let mut writer = BitWriter::new(dst, CHUNK_META_PADDING);
    self.meta.write_to(&mut writer)?;
    Ok(writer.finish())
  }

  fn dissect_unsigneds(&self) -> PcoResult<DissectedSrc<U>> {
    let Self {
      latent_configs,
      src,
      needs_gcds,
      n_nontrivial_latents,
      ..
    } = self;

    let uninit_dissected_latents = |n, ans_default_state| {
      let ans_final_states = [ans_default_state; ANS_INTERLEAVING];
      DissectedLatents {
        ans_vals: uninit_vec(n),
        ans_bits: uninit_vec(n),
        offsets: uninit_vec(n),
        offset_bits: uninit_vec(n),
        ans_final_states,
      }
    };

    let mut res = DissectedSrc {
      page_size: src.page_n,
      dissected_latents: Vec::new(),
    };

    for (latent_idx, config) in latent_configs
      .iter()
      .take(*n_nontrivial_latents)
      .enumerate()
    {
      let latents = &src.latents[latent_idx];
      let LatentConfig { table, encoder, .. } = config;
      let mut dissected_latents = uninit_dissected_latents(latents.len(), encoder.default_state());

      // we go through in reverse for ANS!
      let mut lbd = LatentBatchDissector::new(*needs_gcds, table, encoder);
      for (batch_idx, batch) in latents.chunks(FULL_BATCH_SIZE).enumerate().rev() {
        let base_i = batch_idx * FULL_BATCH_SIZE;
        lbd.dissect_latent_batch(batch, base_i, &mut dissected_latents)
      }
      res.dissected_latents.push(dissected_latents);
    }

    Ok(res)
  }

  /// Returns an estimate of the overall size of a specific page.
  ///
  /// This can be useful when building the file as a `Vec<u8>` in memory;
  /// you can `.reserve(chunk_compressor.chunk_size_hint())` ahead of time.
  pub fn page_size_hint(&self, page_idx: usize) -> usize {
    let page_size = self.page_sizes[page_idx];
    let mut bit_size = 0;
    for (latent_idx, latent_var) in self.meta.latents.iter().enumerate() {
      let meta_bit_size = self.meta.delta_encoding_order * U::BITS as usize
        + ANS_INTERLEAVING * latent_var.ans_size_log as usize;
      let nums_bit_size = page_size * self.max_bits_per_latent[latent_idx] as usize;
      bit_size += meta_bit_size + nums_bit_size;
    }
    bits::ceil_div(bit_size, 8)
  }

  /// Writes a page to the destination.
  ///
  /// Will return an error if the provided `Write` errors.
  pub fn write_page<W: Write>(&self, page_idx: usize, dst: W) -> PcoResult<W> {
    if page_idx >= self.page_sizes.len() {
      return Err(PcoError::invalid_argument(format!(
        "page idx exceeds num pages ({} >= {})",
        page_idx,
        self.page_sizes.len(),
      )));
    }

    let mut writer = BitWriter::new(dst, PAGE_PADDING);

    // TODO why doesn't this take page_idx? Am I doing repeated work
    // (or worse)?
    let dissected_src = self.dissect_unsigneds()?;

    let mut latent_metas = Vec::with_capacity(self.n_latents);
    for latent_idx in 0..self.n_latents {
      let delta_moments = self.page_moments(page_idx, latent_idx).clone();

      let ans_final_state_idxs = dissected_src
        .dissected_latents
        .get(latent_idx)
        .map(|dissected| dissected.ans_final_states)
        .unwrap_or([0; ANS_INTERLEAVING]);
      latent_metas.push(PageLatentMeta {
        delta_moments,
        ans_final_state_idxs,
      });
    }
    let page_meta = PageMeta {
      latents: latent_metas,
    };
    let ans_size_logs = self
      .latent_configs
      .iter()
      .map(|config| config.encoder.size_log());

    page_meta.write_to(ans_size_logs, &mut writer);
    writer.flush()?;

    write_dissecteds(dissected_src, &mut writer)?;

    writer.finish_byte();
    writer.flush()?;
    Ok(writer.finish())
  }
}

#[cfg(test)]
mod tests {
  use crate::wrapped::chunk_compressor::choose_max_n_bins;

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
