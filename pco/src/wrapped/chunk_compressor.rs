use std::cmp::min;
use std::io::Write;

use crate::bin::BinCompressionInfo;
use crate::bit_writer::BitWriter;
use crate::compression_intermediates::{DissectedPage, DissectedPageVar, PageInfo};
use crate::compression_table::CompressionTable;
use crate::constants::{
  Bitlen, Weight, ANS_INTERLEAVING, LIMITED_UNOPTIMIZED_BINS_LOG, MAX_COMPRESSION_LEVEL,
  MAX_DELTA_ENCODING_ORDER, MAX_ENTRIES, OVERSHOOT_PADDING, PAGE_PADDING,
};
use crate::data_types::{Latent, NumberLike};
use crate::delta::DeltaMoments;
use crate::errors::{PcoError, PcoResult};
use crate::histograms::histogram;
use crate::latent_batch_dissector::LatentBatchDissector;
use crate::page_meta::{PageLatentVarMeta, PageMeta};
use crate::read_write_uint::ReadWriteUint;
use crate::wrapped::guarantee;
use crate::{
  ans, bin_optimization, bit_reader, bit_writer, data_types, delta, read_write_uint, Bin,
  ChunkConfig, ChunkLatentVarMeta, ChunkMeta, Mode, PagingSpec, FULL_BATCH_N,
};

// if it looks like the average page of size n will use k bits, hint that it
// will be PAGE_SIZE_OVERESTIMATION * k bits.
const PAGE_SIZE_OVERESTIMATION: f64 = 1.2;
const N_PER_EXTRA_DELTA_GROUP: usize = 10000;
const DELTA_GROUP_SIZE: usize = 200;

// returns table size log
fn quantize_weights<L: Latent>(
  infos: &mut [BinCompressionInfo<L>],
  n_latents: usize,
  estimated_ans_size_log: Bitlen,
) -> Bitlen {
  let counts = infos.iter().map(|info| info.weight).collect::<Vec<_>>();
  let (ans_size_log, weights) = ans::quantize_weights(counts, n_latents, estimated_ans_size_log);

  for (i, weight) in weights.into_iter().enumerate() {
    infos[i].weight = weight;
  }
  ans_size_log
}

#[derive(Default)]
struct TrainedBins<L: Latent> {
  infos: Vec<BinCompressionInfo<L>>,
  ans_size_log: Bitlen,
  counts: Vec<Weight>,
}

fn train_infos<L: Latent>(
  mut latents: Vec<L>,
  unoptimized_bins_log: Bitlen,
) -> PcoResult<TrainedBins<L>> {
  if latents.is_empty() {
    return Ok(TrainedBins::default());
  }

  let n_latents = latents.len();
  let unoptimized_bins = histogram(&mut latents, unoptimized_bins_log as Bitlen);

  let n_log_ceil = if n_latents <= 1 {
    0
  } else {
    (n_latents - 1).ilog2() + 1
  };
  // We cap the ANS table size so that it fits into L1 (or at least L2) cache
  // and has predictably small bitlengths for fast decompression.
  // Maybe in the future we could extend this to MAX_ANS_BITS (14) if the user
  // enables something. We should definitely quantize more aggressively if we
  // do that.
  let estimated_ans_size_log = min(
    min(
      (unoptimized_bins_log + 2) as Bitlen,
      MAX_COMPRESSION_LEVEL as Bitlen,
    ),
    n_log_ceil,
  );

  let mut optimized_infos =
    bin_optimization::optimize_bins(&unoptimized_bins, estimated_ans_size_log);

  let counts = optimized_infos
    .iter()
    .map(|info| info.weight)
    .collect::<Vec<_>>();
  let ans_size_log = quantize_weights(
    &mut optimized_infos,
    n_latents,
    estimated_ans_size_log,
  );

  Ok(TrainedBins {
    infos: optimized_infos,
    ans_size_log,
    counts,
  })
}

fn uninit_vec<T>(n: usize) -> Vec<T> {
  unsafe {
    let mut res = Vec::with_capacity(n);
    res.set_len(n);
    res
  }
}

// This would be very hard to combine with write_uints because it makes use of
// an optimization that only works easily for single-u64 writes of 56 bits or
// less: we keep the `target_u64` value we're updating in a register instead
// of referring back to `dst` (recent values of which will be in L1 cache). If
// a write exceeds 56 bits, we may need to shift target_u64 by 64 bits, which
// would be an overflow panic.
#[inline(never)]
unsafe fn write_short_uints<U: ReadWriteUint>(
  vals: &[U],
  bitlens: &[Bitlen],
  mut stale_byte_idx: usize,
  mut bits_past_byte: Bitlen,
  dst: &mut [u8],
) -> (usize, Bitlen) {
  stale_byte_idx += bits_past_byte as usize / 8;
  bits_past_byte %= 8;
  let mut target_u64 = bit_reader::u64_at(dst, stale_byte_idx);

  for (&val, &bitlen) in vals.iter().zip(bitlens).take(FULL_BATCH_N) {
    let bytes_added = bits_past_byte as usize / 8;
    stale_byte_idx += bytes_added;
    target_u64 >>= bytes_added * 8;
    bits_past_byte %= 8;

    target_u64 |= val.to_u64() << bits_past_byte;
    bit_writer::write_u64_to(target_u64, stale_byte_idx, dst);

    bits_past_byte += bitlen;
  }
  (stale_byte_idx, bits_past_byte)
}

#[inline(never)]
unsafe fn write_uints<U: ReadWriteUint, const MAX_U64S: usize>(
  vals: &[U],
  bitlens: &[Bitlen],
  mut stale_byte_idx: usize,
  mut bits_past_byte: Bitlen,
  dst: &mut [u8],
) -> (usize, Bitlen) {
  for (&val, &bitlen) in vals.iter().zip(bitlens).take(FULL_BATCH_N) {
    stale_byte_idx += bits_past_byte as usize / 8;
    bits_past_byte %= 8;
    bit_writer::write_uint_to::<_, MAX_U64S>(val, stale_byte_idx, bits_past_byte, dst);
    bits_past_byte += bitlen;
  }
  (stale_byte_idx, bits_past_byte)
}

fn write_dissected_batch_var<L: Latent, W: Write>(
  dissected_page_var: &DissectedPageVar<L>,
  var_policy: &LatentVarPolicy<L>,
  batch_start: usize,
  writer: &mut BitWriter<W>,
) -> PcoResult<()> {
  assert!(writer.buf.len() >= PAGE_PADDING);
  writer.flush()?;

  if batch_start >= dissected_page_var.offsets.len() {
    return Ok(());
  }

  // write ANS
  if var_policy.needs_ans {
    (writer.stale_byte_idx, writer.bits_past_byte) = unsafe {
      write_short_uints(
        &dissected_page_var.ans_vals[batch_start..],
        &dissected_page_var.ans_bits[batch_start..],
        writer.stale_byte_idx,
        writer.bits_past_byte,
        &mut writer.buf,
      )
    };
  }

  // write offsets
  (writer.stale_byte_idx, writer.bits_past_byte) = unsafe {
    match var_policy.max_u64s_per_offset {
      0 => (writer.stale_byte_idx, writer.bits_past_byte),
      1 => write_short_uints::<L>(
        &dissected_page_var.offsets[batch_start..],
        &dissected_page_var.offset_bits[batch_start..],
        writer.stale_byte_idx,
        writer.bits_past_byte,
        &mut writer.buf,
      ),
      2 => write_uints::<L, 2>(
        &dissected_page_var.offsets[batch_start..],
        &dissected_page_var.offset_bits[batch_start..],
        writer.stale_byte_idx,
        writer.bits_past_byte,
        &mut writer.buf,
      ),
      3 => write_uints::<L, 3>(
        &dissected_page_var.offsets[batch_start..],
        &dissected_page_var.offset_bits[batch_start..],
        writer.stale_byte_idx,
        writer.bits_past_byte,
        &mut writer.buf,
      ),
      _ => panic!("[ChunkCompressor] data type is too large"),
    }
  };

  Ok(())
}

#[derive(Clone, Debug)]
struct LatentVarPolicy<L: Latent> {
  table: CompressionTable<L>,
  encoder: ans::Encoder,
  avg_bits_per_delta: f64,
  is_trivial: bool,
  needs_ans: bool,
  max_u64s_per_offset: usize,
}

/// Holds metadata about a chunk and supports compression.
#[derive(Clone, Debug)]
pub struct ChunkCompressor<L: Latent> {
  meta: ChunkMeta<L>,
  latent_var_policies: Vec<LatentVarPolicy<L>>,
  page_infos: Vec<PageInfo>,
  // n_latent_vars x n_deltas
  deltas: Vec<Vec<L>>,
  // n_pages x n_latent_vars
  delta_moments: Vec<Vec<DeltaMoments<L>>>,
}

fn bins_from_compression_infos<L: Latent>(infos: &[BinCompressionInfo<L>]) -> Vec<Bin<L>> {
  infos.iter().cloned().map(Bin::from).collect()
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

#[inline(never)]
fn collect_contiguous_deltas<L: Latent>(
  deltas: &[L],
  page_infos: &[PageInfo],
  latent_idx: usize,
) -> Vec<L> {
  let mut res = Vec::with_capacity(deltas.len());
  for page in page_infos {
    res.extend(&deltas[page.start_idx..page.end_idx_per_var[latent_idx]]);
  }
  res
}

fn build_page_infos_and_delta_moments<L: Latent>(
  mode: Mode<L>,
  delta_order: usize,
  n_per_page: &[usize],
  latents: &mut [Vec<L>],
) -> (Vec<PageInfo>, Vec<Vec<DeltaMoments<L>>>) {
  let n_pages = n_per_page.len();
  let mut page_infos = Vec::with_capacity(n_pages);
  let mut delta_moments = vec![Vec::new(); n_pages];

  // delta encoding
  let mut start_idx = 0;
  for (&page_n, delta_moments) in n_per_page.iter().zip(delta_moments.iter_mut()) {
    let mut end_idx_per_var = Vec::new();
    for (latent_var_idx, latents) in latents.iter_mut().enumerate() {
      let var_delta_order = mode.delta_order_for_latent_var(latent_var_idx, delta_order);
      delta_moments.push(delta::encode_in_place(
        &mut latents[start_idx..start_idx + page_n],
        var_delta_order,
      ));
      end_idx_per_var.push(start_idx + page_n.saturating_sub(var_delta_order));
    }
    page_infos.push(PageInfo {
      page_n,
      start_idx,
      end_idx_per_var,
    });

    start_idx += page_n;
  }

  (page_infos, delta_moments)
}

fn new_candidate_w_split_and_delta_order<L: Latent>(
  mut latents: Vec<Vec<L>>, // start out plain, gets delta encoded in place
  paging_spec: &PagingSpec,
  mode: Mode<L>,
  delta_order: usize,
  unoptimized_bins_log: Bitlen,
) -> PcoResult<(ChunkCompressor<L>, Vec<Vec<Weight>>)> {
  let chunk_n = latents[0].len();
  let n_per_page = paging_spec.n_per_page(chunk_n)?;
  let n_latent_vars = mode.n_latent_vars();

  let (page_infos, delta_moments) =
    build_page_infos_and_delta_moments(mode, delta_order, &n_per_page, &mut latents);
  let deltas = latents;

  // training bins
  let mut var_metas = Vec::with_capacity(n_latent_vars);
  let mut var_policies = Vec::with_capacity(n_latent_vars);
  let mut bin_counts = Vec::with_capacity(n_latent_vars);
  for (latent_idx, deltas) in deltas.iter().enumerate() {
    // secondary latents should be compressed faster
    let unoptimized_bins_log = if latent_idx == 0 {
      unoptimized_bins_log
    } else {
      min(
        unoptimized_bins_log,
        LIMITED_UNOPTIMIZED_BINS_LOG,
      )
    };

    let contiguous_deltas = collect_contiguous_deltas(deltas, &page_infos, latent_idx);

    let trained = train_infos(contiguous_deltas, unoptimized_bins_log)?;
    let bins = bins_from_compression_infos(&trained.infos);
    let needs_ans = bins.len() != 1;

    let table = CompressionTable::from(trained.infos);
    let encoder = ans::Encoder::from_bins(trained.ans_size_log, &bins)?;

    let latent_meta = ChunkLatentVarMeta {
      bins,
      ans_size_log: trained.ans_size_log,
    };
    let max_bits_per_offset = latent_meta.max_bits_per_offset();
    let avg_bits_per_delta = latent_meta.avg_bits_per_delta();
    let is_trivial = latent_meta.is_trivial();

    let max_u64s_per_offset = read_write_uint::calc_max_u64s_for_writing(max_bits_per_offset);

    var_metas.push(latent_meta);
    var_policies.push(LatentVarPolicy {
      table,
      encoder,
      avg_bits_per_delta,
      is_trivial,
      needs_ans,
      max_u64s_per_offset,
    });
    bin_counts.push(trained.counts);
  }

  let meta = ChunkMeta::new(mode, delta_order, var_metas);
  let chunk_compressor = ChunkCompressor {
    meta,
    latent_var_policies: var_policies,
    page_infos,
    deltas,
    delta_moments,
  };

  Ok((chunk_compressor, bin_counts))
}

fn choose_delta_sample<L: Latent>(
  primary_latents: &[L],
  group_size: usize,
  n_extra_groups: usize,
) -> Vec<L> {
  let n = primary_latents.len();
  let nominal_sample_size = (n_extra_groups + 1) * group_size;
  let mut sample = Vec::with_capacity(nominal_sample_size);
  let group_padding = if n_extra_groups == 0 {
    0
  } else {
    n.saturating_sub(nominal_sample_size) / n_extra_groups
  };

  sample.extend(primary_latents.iter().take(group_size));
  let mut i = group_size;
  for _ in 0..n_extra_groups {
    i += group_padding;
    sample.extend(primary_latents.iter().skip(i).take(group_size));
    i += group_size;
  }

  sample
}

// Right now this is entirely based on the primary latents since no existing
// modes apply deltas to secondary latents. Might want to change this
// eventually?
#[inline(never)]
fn choose_delta_encoding_order<L: Latent>(
  primary_latents: &[L],
  unoptimized_bins_log: Bitlen,
) -> PcoResult<usize> {
  let sample = choose_delta_sample(
    primary_latents,
    DELTA_GROUP_SIZE,
    1 + primary_latents.len() / N_PER_EXTRA_DELTA_GROUP,
  );

  let mut best_order = usize::MAX;
  let mut best_size = usize::MAX;
  for delta_encoding_order in 0..MAX_DELTA_ENCODING_ORDER + 1 {
    let (sample_cc, _) = new_candidate_w_split_and_delta_order(
      vec![sample.clone()],
      &PagingSpec::Exact(vec![sample.len()]),
      Mode::Classic,
      delta_encoding_order,
      unoptimized_bins_log,
    )?;
    let size_estimate = sample_cc.chunk_meta_size_hint() + sample_cc.page_size_hint_inner(0, 1.0);
    if size_estimate < best_size {
      best_order = delta_encoding_order;
      best_size = size_estimate;
    } else {
      // it's almost always convex
      break;
    }
  }

  Ok(best_order)
}

fn choose_unoptimized_bins_log(compression_level: usize, n: usize) -> Bitlen {
  let compression_level = compression_level as Bitlen;
  let log_n = (n as f64).log2().floor() as Bitlen;
  let fast_unoptimized_bins_log = log_n.saturating_sub(4);
  if compression_level <= fast_unoptimized_bins_log {
    compression_level
  } else {
    fast_unoptimized_bins_log + compression_level.saturating_sub(fast_unoptimized_bins_log) / 2
  }
}

// We pull this stuff out of `new` because it only depends on the latent type
// and we don't need a specialization for each full dtype.
// Returns a chunk compressor and the counts (per latent var) of numbers in
// each bin.
fn new_candidate_w_split<L: Latent>(
  mode: Mode<L>,
  latents: Vec<Vec<L>>,
  config: &ChunkConfig,
) -> PcoResult<(ChunkCompressor<L>, Vec<Vec<Weight>>)> {
  let unoptimized_bins_log =
    choose_unoptimized_bins_log(config.compression_level, latents[0].len());
  let delta_order = if let Some(delta_order) = config.delta_encoding_order {
    delta_order
  } else {
    choose_delta_encoding_order(&latents[0], unoptimized_bins_log)?
  };

  new_candidate_w_split_and_delta_order(
    latents,
    &config.paging_spec,
    mode,
    delta_order,
    unoptimized_bins_log,
  )
}

fn should_fallback<L: Latent>(
  n: usize,
  candidate: &ChunkCompressor<L>,
  bin_counts_per_latent_var: Vec<Vec<Weight>>,
) -> bool {
  let meta = &candidate.meta;
  if meta.delta_encoding_order == 0 && matches!(meta.mode, Mode::Classic) {
    // we already have a size guarantee in this case
    return false;
  }

  let n_pages = candidate.page_infos.len();

  // worst case trailing bytes after bit packing
  let mut worst_case_body_bit_size = 7 * n_pages;
  for (latent_var_meta, bin_counts) in meta
    .per_latent_var
    .iter()
    .zip(bin_counts_per_latent_var.iter())
  {
    for (bin, &count) in latent_var_meta.bins.iter().zip(bin_counts) {
      worst_case_body_bit_size +=
        count as usize * bin.worst_case_bits_per_delta(latent_var_meta.ans_size_log) as usize;
    }
  }

  let worst_case_size = meta.exact_size()
    + n_pages * meta.exact_page_meta_size()
    + worst_case_body_bit_size.div_ceil(8);
  let baseline_size = guarantee::chunk_size::<L>(n);
  worst_case_size > baseline_size
}

fn fallback_chunk_compressor<L: Latent>(
  mut latents: Vec<Vec<L>>,
  config: &ChunkConfig,
) -> PcoResult<ChunkCompressor<L>> {
  let n_per_page = config.paging_spec.n_per_page(latents[0].len())?;
  let (page_infos, delta_moments) =
    build_page_infos_and_delta_moments(Mode::Classic, 0, &n_per_page, &mut latents);
  let infos = vec![BinCompressionInfo::<L> {
    weight: 1,
    symbol: 0,
    ..Default::default()
  }];
  let ans_size_log = 0;
  let bins = bins_from_compression_infos(&infos);
  let table = CompressionTable::from(infos);
  let encoder = ans::Encoder::from_bins(ans_size_log, &bins)?;
  let offset_bits = L::BITS;
  let max_u64s_per_offset = read_write_uint::calc_max_u64s_for_writing(offset_bits);

  let policy = LatentVarPolicy {
    table,
    encoder,
    avg_bits_per_delta: offset_bits as f64,
    is_trivial: false,
    needs_ans: false,
    max_u64s_per_offset,
  };
  Ok(ChunkCompressor {
    meta: guarantee::baseline_chunk_meta::<L>(),
    latent_var_policies: vec![policy],
    page_infos,
    deltas: latents,
    delta_moments,
  })
}

// Should this take nums as a slice of slices instead of having a config.paging_spec?
pub(crate) fn new<T: NumberLike>(
  nums: &[T],
  config: &ChunkConfig,
) -> PcoResult<ChunkCompressor<T::L>> {
  validate_config(config)?;
  let n = nums.len();
  validate_chunk_size(n)?;

  let (mode, latents) = T::choose_mode_and_split_latents(nums, config);

  let (candidate, bin_counts) = new_candidate_w_split(mode, latents, config)?;
  if should_fallback(n, &candidate, bin_counts) {
    let latents = data_types::split_latents_classic(nums);
    return fallback_chunk_compressor(latents, config);
  }

  Ok(candidate)
}

impl<L: Latent> ChunkCompressor<L> {
  fn page_moments(&self, page_idx: usize, latent_var_idx: usize) -> &DeltaMoments<L> {
    &self.delta_moments[page_idx][latent_var_idx]
  }

  /// Returns the count of numbers this chunk will contain in each page.
  pub fn n_per_page(&self) -> Vec<usize> {
    self.page_infos.iter().map(|page| page.page_n).collect()
  }

  /// Returns pre-computed information about the chunk.
  pub fn meta(&self) -> &ChunkMeta<L> {
    &self.meta
  }

  /// Returns an estimate of the overall size of the chunk.
  ///
  /// This can be useful when building the file as a `Vec<u8>` in memory;
  /// you can `.reserve()` ahead of time.
  pub fn chunk_meta_size_hint(&self) -> usize {
    self.meta.exact_size()
  }

  /// Writes the chunk metadata to the destination.
  ///
  /// Will return an error if the provided `Write` errors.
  pub fn write_chunk_meta<W: Write>(&self, dst: W) -> PcoResult<W> {
    let mut writer = BitWriter::new(
      dst,
      self.meta.exact_size() + OVERSHOOT_PADDING,
    );
    unsafe { self.meta.write_to(&mut writer)? };
    Ok(writer.into_inner())
  }

  fn dissect_page(&self, page_idx: usize) -> PcoResult<DissectedPage<L>> {
    let Self {
      latent_var_policies,
      deltas,
      page_infos,
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

    let page_info = &page_infos[page_idx];
    let mut per_var = Vec::new();

    for ((var_policy, &delta_end), var_deltas) in latent_var_policies
      .iter()
      .zip(page_info.end_idx_per_var.iter())
      .zip(deltas)
    {
      if var_policy.is_trivial {
        per_var.push(uninit_dissected_page_var(
          0,
          var_policy.encoder.default_state(),
        ));
        continue;
      }

      let page_deltas = &var_deltas[page_info.start_idx..delta_end];
      let LatentVarPolicy { table, encoder, .. } = var_policy;
      let mut dissected_page_var =
        uninit_dissected_page_var(page_deltas.len(), encoder.default_state());

      // we go through in reverse for ANS!
      let mut lbd = LatentBatchDissector::new(table, encoder);
      for (batch_idx, batch) in page_deltas.chunks(FULL_BATCH_N).enumerate().rev() {
        let base_i = batch_idx * FULL_BATCH_N;
        lbd.dissect_latent_batch(batch, base_i, &mut dissected_page_var)
      }
      per_var.push(dissected_page_var);
    }

    Ok(DissectedPage {
      page_n: page_info.page_n,
      per_var,
    })
  }

  /// Returns an estimate of the overall size of a specific page.
  ///
  /// This can be useful when building the file as a `Vec<u8>` in memory;
  /// you can `.reserve(chunk_compressor.chunk_size_hint())` ahead of time.
  pub fn page_size_hint(&self, page_idx: usize) -> usize {
    self.page_size_hint_inner(page_idx, PAGE_SIZE_OVERESTIMATION)
  }

  fn page_size_hint_inner(&self, page_idx: usize, page_size_overestimation: f64) -> usize {
    let page_info = &self.page_infos[page_idx];
    let mut body_bit_size = 0;
    for (var_policy, &end_idx) in self
      .latent_var_policies
      .iter()
      .zip(&page_info.end_idx_per_var)
    {
      let page_n_deltas = end_idx - page_info.start_idx;
      let nums_bit_size = page_n_deltas as f64 * var_policy.avg_bits_per_delta;
      body_bit_size += (nums_bit_size * page_size_overestimation).ceil() as usize;
    }
    self.meta.exact_page_meta_size() + body_bit_size.div_ceil(8)
  }

  #[inline(never)]
  fn write_dissected_page<W: Write>(
    &self,
    dissected_page: DissectedPage<L>,
    writer: &mut BitWriter<W>,
  ) -> PcoResult<()> {
    let mut batch_start = 0;
    while batch_start < dissected_page.page_n {
      let batch_end = min(
        batch_start + FULL_BATCH_N,
        dissected_page.page_n,
      );
      for (dissected_page_var, policy) in
        dissected_page.per_var.iter().zip(&self.latent_var_policies)
      {
        write_dissected_batch_var(
          dissected_page_var,
          policy,
          batch_start,
          writer,
        )?;
      }
      batch_start = batch_end;
    }
    Ok(())
  }

  /// Writes a page to the destination.
  ///
  /// Will return an error if the provided `Write` errors.
  pub fn write_page<W: Write>(&self, page_idx: usize, dst: W) -> PcoResult<W> {
    let n_pages = self.page_infos.len();
    if page_idx >= n_pages {
      return Err(PcoError::invalid_argument(format!(
        "page idx exceeds num pages ({} >= {})",
        page_idx, n_pages,
      )));
    }

    let mut writer = BitWriter::new(dst, PAGE_PADDING);

    let dissected_page = self.dissect_page(page_idx)?;

    let n_latents = self.meta.mode.n_latent_vars();
    let mut latent_metas = Vec::with_capacity(n_latents);
    for latent_idx in 0..n_latents {
      let delta_moments = self.page_moments(page_idx, latent_idx).clone();
      let base_state = self.latent_var_policies[latent_idx].encoder.default_state();

      let ans_final_state_idxs = dissected_page
        .per_var
        .get(latent_idx)
        .map(|dissected| dissected.ans_final_states.map(|state| state - base_state))
        .unwrap_or([0; ANS_INTERLEAVING]);
      latent_metas.push(PageLatentVarMeta {
        delta_moments,
        ans_final_state_idxs,
      });
    }
    let page_meta = PageMeta {
      per_var: latent_metas,
    };
    let ans_size_logs = self
      .latent_var_policies
      .iter()
      .map(|config| config.encoder.size_log());

    unsafe { page_meta.write_to(ans_size_logs, &mut writer) };

    self.write_dissected_page(dissected_page, &mut writer)?;

    writer.finish_byte();
    writer.flush()?;
    Ok(writer.into_inner())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_choose_delta_sample() {
    let latents = vec![0_u32, 1];
    assert_eq!(
      choose_delta_sample(&latents, 100, 0),
      vec![0, 1]
    );
    assert_eq!(
      choose_delta_sample(&latents, 100, 1),
      vec![0, 1]
    );

    let latents = (0..300).collect::<Vec<u32>>();
    let sample = choose_delta_sample(&latents, 100, 1);
    assert_eq!(sample.len(), 200);
    assert_eq!(&sample[..3], &[0, 1, 2]);
    assert_eq!(&sample[197..], &[297, 298, 299]);

    let latents = (0..8).collect::<Vec<u32>>();
    assert_eq!(
      choose_delta_sample(&latents, 2, 2),
      vec![0, 1, 3, 4, 6, 7]
    );
  }
}
