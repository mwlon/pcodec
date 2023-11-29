use std::cmp::{max, min};
use std::io::Write;

use crate::bin::BinCompressionInfo;
use crate::bit_writer::BitWriter;
use crate::compression_intermediates::{DissectedPage, DissectedPageVar, PageLatents};
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
use crate::page_meta::{PageLatentVarMeta, PageMeta};
use crate::{
  ans, bin_optimization, bits, delta, float_mult_utils, int_mult_utils, Bin, ChunkConfig,
  ChunkLatentVarMeta, ChunkMeta, FloatMultSpec, IntMultSpec, Mode, FULL_BATCH_N,
};

struct BinBuffer<'a, U: UnsignedLike> {
  pub seq: Vec<BinCompressionInfo<U>>,
  bin_idx: usize,
  max_n_bin: usize,
  n_unsigneds: usize,
  sorted: &'a [U],
  pub target_j: usize,
}

impl<'a, U: UnsignedLike> BinBuffer<'a, U> {
  fn calc_target_j(&mut self) {
    self.target_j = ((self.bin_idx + 1) * self.n_unsigneds) / self.max_n_bin
  }

  fn new(max_n_bin: usize, n_unsigneds: usize, sorted: &'a [U]) -> Self {
    let mut res = Self {
      seq: Vec::with_capacity(max_n_bin),
      bin_idx: 0,
      max_n_bin,
      n_unsigneds,
      sorted,
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

    let bin = BinCompressionInfo {
      weight: count as Weight,
      lower,
      upper,
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

fn choose_unoptimized_bins<U: UnsignedLike>(
  sorted: &[U],
  comp_level: usize,
) -> Vec<BinCompressionInfo<U>> {
  let n_unsigneds = sorted.len();
  let max_n_bin = choose_max_n_bins(comp_level, n_unsigneds);

  let mut i = 0;
  let mut backup_j = 0_usize;
  let mut bin_buffer = BinBuffer::<U>::new(max_n_bin, n_unsigneds, sorted);

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

  bin_buffer.seq
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

fn train_infos<U: UnsignedLike>(
  unsigneds: Vec<U>,
  comp_level: usize,
  n: usize, // can be greater than unsigneds.len() if delta encoding is on
) -> PcoResult<TrainedBins<U>> {
  if unsigneds.is_empty() {
    return Ok(TrainedBins::default());
  }

  let n_unsigneds = unsigneds.len();
  let unoptimized_bins = {
    let mut sorted = unsigneds;
    sorted.sort_unstable();
    choose_unoptimized_bins(&sorted, comp_level)
  };

  let estimated_ans_size_log = (comp_level + 2) as Bitlen;
  let mut optimized_infos =
    bin_optimization::optimize_bins(unoptimized_bins, estimated_ans_size_log, n);

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

#[inline(never)]
fn write_dissected_page<U: UnsignedLike, W: Write>(
  dissected_page: DissectedPage<U>,
  writer: &mut BitWriter<W>,
) -> PcoResult<()> {
  let mut batch_start = 0;
  while batch_start < dissected_page.page_n {
    let batch_end = min(
      batch_start + FULL_BATCH_N,
      dissected_page.page_n,
    );
    for dissected_page_var in &dissected_page.per_var {
      for (&val, &bits) in dissected_page_var
        .ans_vals
        .iter()
        .zip(dissected_page_var.ans_bits.iter())
        .skip(batch_start)
        .take(FULL_BATCH_N)
      {
        writer.write_uint(val, bits);
      }
      for (&offset, &bits) in dissected_page_var
        .offsets
        .iter()
        .zip(dissected_page_var.offset_bits.iter())
        .skip(batch_start)
        .take(FULL_BATCH_N)
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
struct LatentVarPolicy<U: UnsignedLike> {
  table: CompressionTable<U>,
  encoder: ans::Encoder,
  max_bits_per_latent: Bitlen,
  is_trivial: bool,
}

/// Holds metadata about a chunk and supports compression.
#[derive(Clone, Debug)]
pub struct ChunkCompressor<U: UnsignedLike> {
  meta: ChunkMeta<U>,
  latent_var_policies: Vec<LatentVarPolicy<U>>,
  paginated_latents: Vec<PageLatents<U>>,
}

fn bins_from_compression_infos<U: UnsignedLike>(infos: &[BinCompressionInfo<U>]) -> Vec<Bin<U>> {
  infos.iter().cloned().map(Bin::from).collect()
}

fn choose_naive_mode<T: NumberLike>(nums: &[T], config: &ChunkConfig) -> Mode<T::Unsigned> {
  // * Use float mult if enabled and an appropriate base is found
  // * Otherwise, use int mult if enabled and an appropriate int mult is found
  // * Otherwise, use Classic
  if matches!(
    config.float_mult_spec,
    FloatMultSpec::Enabled
  ) && T::IS_FLOAT
  {
    if let Some(config) = float_mult_utils::choose_config(nums) {
      return Mode::FloatMult(config);
    }
  }

  if matches!(config.int_mult_spec, IntMultSpec::Enabled) && !T::IS_FLOAT {
    if let Some(base) = int_mult_utils::choose_base(nums) {
      return Mode::IntMult(base);
    }
  }

  Mode::Classic
}

#[inline(never)]
fn split_latents<T: NumberLike>(
  naive_mode: Mode<T::Unsigned>,
  page_nums: &[T],
) -> PageLatents<T::Unsigned> {
  match naive_mode {
    Mode::Classic => PageLatents::new_pre_delta(vec![page_nums
      .iter()
      .map(|x| x.to_unsigned())
      .collect()]),
    Mode::FloatMult(FloatMultConfig { base, inv_base }) => {
      float_mult_utils::split_latents(page_nums, base, inv_base)
    }
    Mode::IntMult(base) => int_mult_utils::split_latents(page_nums, base),
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

// We pull this stuff out of `new` because it only depends on the unsigned type
// and we don't need a specialization for each full dtype.
fn unsigned_new<U: UnsignedLike>(
  mut paginated_latents: Vec<PageLatents<U>>,
  config: &ChunkConfig,
  mode: Mode<U>,
  delta_order: usize,
) -> PcoResult<ChunkCompressor<U>> {
  let n_latents = mode.n_latent_vars();
  let mut var_metas = Vec::with_capacity(n_latents);
  let mut var_policies = Vec::with_capacity(n_latents);
  let chunk_n = paginated_latents.iter().map(|page| page.page_n).sum();

  // delta encoding
  for latent_page in &mut paginated_latents {
    for (latent_idx, page_var_latents) in latent_page.per_var.iter_mut().enumerate() {
      let var_delta_order = mode.delta_order_for_latent_var(latent_idx, delta_order);
      page_var_latents.delta_moments = delta::encode_in_place(
        &mut page_var_latents.latents,
        var_delta_order,
      );
    }
  }

  // training bins
  for latent_idx in 0..n_latents {
    // secondary latents should be compressed faster
    let comp_level = if latent_idx == 0 {
      config.compression_level
    } else {
      min(config.compression_level, 5)
    };

    let contiguous_latents = paginated_latents
      .iter()
      .flat_map(|page_latents| &page_latents.per_var[latent_idx].latents)
      .copied()
      .collect::<Vec<_>>();

    let trained = train_infos(contiguous_latents, comp_level, chunk_n)?;
    let bins = bins_from_compression_infos(&trained.infos);

    let table = CompressionTable::from(trained.infos);
    let encoder = ans::Encoder::from_bins(trained.ans_size_log, &bins)?;

    let latent_meta = ChunkLatentVarMeta {
      bins,
      ans_size_log: trained.ans_size_log,
    };
    // TODO this bound could be lower. We're probably better off with something
    // more like an expected size though.
    let max_bits_per_latent = latent_meta.max_bits_per_ans() + latent_meta.max_bits_per_offset();
    let is_trivial = latent_meta.is_trivial();

    var_metas.push(latent_meta);
    var_policies.push(LatentVarPolicy {
      table,
      encoder,
      max_bits_per_latent,
      is_trivial,
    });
  }

  let meta = ChunkMeta::new(mode, delta_order, var_metas);

  Ok(ChunkCompressor {
    meta,
    latent_var_policies: var_policies,
    paginated_latents,
  })
}

// Should this take nums as a slice of slices instead of having a config.paging_spec?
pub(crate) fn new<T: NumberLike>(
  nums: &[T],
  config: &ChunkConfig,
) -> PcoResult<ChunkCompressor<T::Unsigned>> {
  validate_config(config)?;
  let n = nums.len();
  validate_chunk_size(n)?;

  let n_per_page = config.paging_spec.n_per_page(n)?;

  let naive_mode = choose_naive_mode(nums, config);
  let mut paginated_latents = Vec::with_capacity(n_per_page.len());
  let mut page_start = 0;
  for &page_n in &n_per_page {
    let page_end = page_start + page_n;
    paginated_latents.push(split_latents(
      naive_mode,
      &nums[page_start..page_end],
    ));
    page_start = page_end;
  }

  let delta_order = if let Some(delta_order) = config.delta_encoding_order {
    delta_order
  } else {
    crate::auto_delta_encoding_order(nums, config.compression_level)?
  };

  unsigned_new(
    paginated_latents,
    config,
    naive_mode,
    delta_order,
  )
}

impl<U: UnsignedLike> ChunkCompressor<U> {
  fn page_moments(&self, page_idx: usize, latent_idx: usize) -> &DeltaMoments<U> {
    &self.paginated_latents[page_idx].per_var[latent_idx].delta_moments
  }

  /// Returns the count of numbers this chunk will contain in each page.
  pub fn n_per_page(&self) -> Vec<usize> {
    self
      .paginated_latents
      .iter()
      .map(|page| page.page_n)
      .collect()
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
    let mut size = 32;
    let bytes_per_num = U::BITS / 8;
    for latent_meta in &self.meta.per_latent_var {
      size += latent_meta.bins.len() * (4 + 2 * bytes_per_num as usize)
    }
    size
  }

  /// Writes the chunk metadata to the destination.
  ///
  /// Will return an error if the provided `Write` errors.
  pub fn write_chunk_meta<W: Write>(&self, dst: W) -> PcoResult<W> {
    let mut writer = BitWriter::new(dst, CHUNK_META_PADDING);
    self.meta.write_to(&mut writer)?;
    Ok(writer.into_inner())
  }

  fn dissect_unsigneds(&self, page_idx: usize) -> PcoResult<DissectedPage<U>> {
    let Self {
      latent_var_policies,
      paginated_latents,
      ..
    } = self;

    let uninit_dissected_page_var = |n, ans_default_state| {
      let ans_final_states = [ans_default_state; ANS_INTERLEAVING];
      DissectedPageVar {
        ans_vals: uninit_vec(n),
        ans_bits: uninit_vec(n),
        offsets: uninit_vec(n),
        offset_bits: uninit_vec(n),
        ans_final_states,
      }
    };

    let page_latents = &paginated_latents[page_idx];
    let mut per_var = Vec::new();

    for (var_policy, var_latents) in latent_var_policies.iter().zip(page_latents.per_var.iter()) {
      if var_policy.is_trivial {
        per_var.push(uninit_dissected_page_var(
          0,
          var_policy.encoder.default_state(),
        ));
        continue;
      }

      let latents = &var_latents.latents;
      let LatentVarPolicy { table, encoder, .. } = var_policy;
      let mut dissected_page_var =
        uninit_dissected_page_var(latents.len(), encoder.default_state());

      // we go through in reverse for ANS!
      let mut lbd = LatentBatchDissector::new(table, encoder);
      for (batch_idx, batch) in latents.chunks(FULL_BATCH_N).enumerate().rev() {
        let base_i = batch_idx * FULL_BATCH_N;
        lbd.dissect_latent_batch(batch, base_i, &mut dissected_page_var)
      }
      per_var.push(dissected_page_var);
    }

    Ok(DissectedPage {
      page_n: page_latents.page_n,
      per_var,
    })
  }

  /// Returns an estimate of the overall size of a specific page.
  ///
  /// This can be useful when building the file as a `Vec<u8>` in memory;
  /// you can `.reserve(chunk_compressor.chunk_size_hint())` ahead of time.
  pub fn page_size_hint(&self, page_idx: usize) -> usize {
    let page_n = self.paginated_latents[page_idx].page_n;
    let mut bit_size = 0;
    for (var_meta, var_policy) in self
      .meta
      .per_latent_var
      .iter()
      .zip(self.latent_var_policies.iter())
    {
      let meta_bit_size = self.meta.delta_encoding_order * U::BITS as usize
        + ANS_INTERLEAVING * var_meta.ans_size_log as usize;
      let nums_bit_size = page_n * var_policy.max_bits_per_latent as usize;
      bit_size += meta_bit_size + nums_bit_size;
    }
    bits::ceil_div(bit_size, 8)
  }

  /// Writes a page to the destination.
  ///
  /// Will return an error if the provided `Write` errors.
  pub fn write_page<W: Write>(&self, page_idx: usize, dst: W) -> PcoResult<W> {
    let n_pages = self.paginated_latents.len();
    if page_idx >= n_pages {
      return Err(PcoError::invalid_argument(format!(
        "page idx exceeds num pages ({} >= {})",
        page_idx, n_pages,
      )));
    }

    let mut writer = BitWriter::new(dst, PAGE_PADDING);

    let dissected_page = self.dissect_unsigneds(page_idx)?;

    let n_latents = self.meta.mode.n_latent_vars();
    let mut latent_metas = Vec::with_capacity(n_latents);
    for latent_idx in 0..n_latents {
      let delta_moments = self.page_moments(page_idx, latent_idx).clone();

      let ans_final_state_idxs = dissected_page
        .per_var
        .get(latent_idx)
        .map(|dissected| dissected.ans_final_states)
        .unwrap_or([0; ANS_INTERLEAVING]);
      latent_metas.push(PageLatentVarMeta {
        delta_moments,
        ans_final_state_idxs,
      });
    }
    let page_meta = PageMeta {
      per_latent_var: latent_metas,
    };
    let ans_size_logs = self
      .latent_var_policies
      .iter()
      .map(|config| config.encoder.size_log());

    page_meta.write_to(ans_size_logs, &mut writer);
    writer.flush()?;

    write_dissected_page(dissected_page, &mut writer)?;

    writer.finish_byte();
    writer.flush()?;
    Ok(writer.into_inner())
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
