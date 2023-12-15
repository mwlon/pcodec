use std::cmp::{max, min};
use std::io::Write;

use crate::bin::BinCompressionInfo;
use crate::bit_writer::BitWriter;
use crate::compression_intermediates::{DissectedPage, DissectedPageVar, PageInfo};
use crate::compression_table::CompressionTable;
use crate::constants::{
  Bitlen, Weight, ANS_INTERLEAVING, CHUNK_META_PADDING, LIMITED_UNOPTIMIZED_BINS_LOG,
  MAX_COMPRESSION_LEVEL, MAX_DELTA_ENCODING_ORDER, MAX_ENTRIES, PAGE_PADDING,
};
use crate::data_types::{NumberLike, UnsignedLike};
use crate::delta::DeltaMoments;
use crate::errors::{PcoError, PcoResult};
use crate::float_mult_utils::FloatMultConfig;
use crate::latent_batch_dissector::LatentBatchDissector;
use crate::page_meta::{PageLatentVarMeta, PageMeta};
use crate::read_write_uint::ReadWriteUint;
use crate::{
  ans, bin_optimization, bit_reader, bit_writer, bits, delta, float_mult_utils, int_mult_utils,
  read_write_uint, Bin, ChunkConfig, ChunkLatentVarMeta, ChunkMeta, FloatMultSpec, IntMultSpec,
  Mode, PagingSpec, FULL_BATCH_N,
};

// if it looks like the average page of size n will use k bits, hint that it
// will be PAGE_SIZE_OVERESTIMATION * k bits.
const PAGE_SIZE_OVERESTIMATION: f64 = 1.2;
const N_PER_EXTRA_DELTA_GROUP: usize = 10000;
const DELTA_GROUP_SIZE: usize = 200;

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
      offset_bits: bits::bits_to_encode_offset(upper - lower),
      ..Default::default()
    };
    self.seq.push(bin);
    self.bin_idx = new_bin_idx;
    self.calc_target_j();
  }
}

#[inline(never)]
fn choose_unoptimized_bins<U: UnsignedLike>(
  sorted: &[U],
  unoptimized_bins_log: usize,
) -> Vec<BinCompressionInfo<U>> {
  let n_unsigneds = sorted.len();
  let max_n_bins = min(1 << unoptimized_bins_log, n_unsigneds);

  let mut i = 0;
  let mut backup_j = 0_usize;
  let mut bin_buffer = BinBuffer::<U>::new(max_n_bins, n_unsigneds, sorted);

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
  estimated_ans_size_log: Bitlen,
) -> Bitlen {
  let counts = infos.iter().map(|info| info.weight).collect::<Vec<_>>();
  let (ans_size_log, weights) = ans::quantize_weights(counts, n_unsigneds, estimated_ans_size_log);

  for (i, weight) in weights.into_iter().enumerate() {
    infos[i].weight = weight;
  }
  ans_size_log
}

#[derive(Default)]
struct TrainedBins<U: UnsignedLike> {
  infos: Vec<BinCompressionInfo<U>>,
  ans_size_log: Bitlen,
}

fn train_infos<U: UnsignedLike>(
  unsigneds: Vec<U>,
  unoptimized_bins_log: usize,
) -> PcoResult<TrainedBins<U>> {
  if unsigneds.is_empty() {
    return Ok(TrainedBins::default());
  }

  let n_unsigneds = unsigneds.len();
  let unoptimized_bins = {
    let mut sorted = unsigneds;
    sorted.sort_unstable();
    choose_unoptimized_bins(&sorted, unoptimized_bins_log)
  };

  let n_log_ceil = if n_unsigneds <= 1 {
    0
  } else {
    (n_unsigneds - 1).ilog2() + 1
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

  let mut optimized_infos = bin_optimization::optimize_bins(
    unoptimized_bins,
    estimated_ans_size_log,
    n_unsigneds as Weight,
  );

  let ans_size_log = quantize_weights(
    &mut optimized_infos,
    n_unsigneds,
    estimated_ans_size_log,
  );

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

// This would be very hard to combine with write_uints because it makes use of
// an optimization that only works easily for single-u64 writes of 56 bits or
// less: we keep the `target_u64` value we're updating in a register instead
// of referring back to `dst` (recent values of which will be in L1 cache). If
// a write exceeds 56 bits, we may need to shift target_u64 by 64 bits, which
// would be an overflow panic.
#[inline(never)]
fn write_short_uints<U: ReadWriteUint>(
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
fn write_uints<U: ReadWriteUint, const MAX_U64S: usize>(
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

fn write_dissected_batch_var<U: UnsignedLike, W: Write>(
  dissected_page_var: &DissectedPageVar<U>,
  var_policy: &LatentVarPolicy<U>,
  batch_start: usize,
  writer: &mut BitWriter<W>,
) {
  if batch_start >= dissected_page_var.offsets.len() {
    return;
  }

  // write ANS
  if var_policy.needs_ans {
    (writer.stale_byte_idx, writer.bits_past_byte) = write_short_uints(
      &dissected_page_var.ans_vals[batch_start..],
      &dissected_page_var.ans_bits[batch_start..],
      writer.stale_byte_idx,
      writer.bits_past_byte,
      &mut writer.buf,
    );
  }

  // write offsets
  (writer.stale_byte_idx, writer.bits_past_byte) = match var_policy.max_u64s_per_offset {
    0 => (writer.stale_byte_idx, writer.bits_past_byte),
    1 => write_short_uints::<U>(
      &dissected_page_var.offsets[batch_start..],
      &dissected_page_var.offset_bits[batch_start..],
      writer.stale_byte_idx,
      writer.bits_past_byte,
      &mut writer.buf,
    ),
    2 => write_uints::<U, 2>(
      &dissected_page_var.offsets[batch_start..],
      &dissected_page_var.offset_bits[batch_start..],
      writer.stale_byte_idx,
      writer.bits_past_byte,
      &mut writer.buf,
    ),
    3 => write_uints::<U, 3>(
      &dissected_page_var.offsets[batch_start..],
      &dissected_page_var.offset_bits[batch_start..],
      writer.stale_byte_idx,
      writer.bits_past_byte,
      &mut writer.buf,
    ),
    _ => panic!("[ChunkCompressor] data type is too large"),
  };
}

#[derive(Clone, Debug)]
struct LatentVarPolicy<U: UnsignedLike> {
  table: CompressionTable<U>,
  encoder: ans::Encoder,
  avg_bits_per_delta: f64,
  is_trivial: bool,
  needs_ans: bool,
  max_u64s_per_offset: usize,
}

/// Holds metadata about a chunk and supports compression.
#[derive(Clone, Debug)]
pub struct ChunkCompressor<U: UnsignedLike> {
  meta: ChunkMeta<U>,
  latent_var_policies: Vec<LatentVarPolicy<U>>,
  page_infos: Vec<PageInfo>,
  // n_latent_vars x n_deltas
  deltas: Vec<Vec<U>>,
  // n_pages x n_latent_vars
  delta_moments: Vec<Vec<DeltaMoments<U>>>,
}

fn bins_from_compression_infos<U: UnsignedLike>(infos: &[BinCompressionInfo<U>]) -> Vec<Bin<U>> {
  infos.iter().cloned().map(Bin::from).collect()
}

fn choose_mode<T: NumberLike>(nums: &[T], config: &ChunkConfig) -> Mode<T::Unsigned> {
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

// returns a long vec of latents per latent variable
#[inline(never)]
fn split_latents<T: NumberLike>(mode: Mode<T::Unsigned>, page_nums: &[T]) -> Vec<Vec<T::Unsigned>> {
  match mode {
    Mode::Classic => vec![page_nums.iter().map(|x| x.to_unsigned()).collect()],
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

#[inline(never)]
fn collect_contiguous_deltas<U: UnsignedLike>(
  deltas: &[U],
  page_infos: &[PageInfo],
  latent_idx: usize,
) -> Vec<U> {
  let mut res = Vec::with_capacity(deltas.len());
  for page in page_infos {
    res.extend(&deltas[page.start_idx..page.end_idx_per_var[latent_idx]]);
  }
  res
}

fn unsigned_new_w_delta_order<U: UnsignedLike>(
  mut latents: Vec<Vec<U>>, // start out plain, gets delta encoded in place
  paging_spec: &PagingSpec,
  mode: Mode<U>,
  delta_order: usize,
  unoptimized_bins_log: usize,
) -> PcoResult<ChunkCompressor<U>> {
  let chunk_n = latents[0].len();
  let n_per_page = paging_spec.n_per_page(chunk_n)?;
  let n_pages = n_per_page.len();
  let n_latents = mode.n_latent_vars();

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
  let deltas = latents;

  // training bins
  let mut var_metas = Vec::with_capacity(n_latents);
  let mut var_policies = Vec::with_capacity(n_latents);
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

    let mut max_u64s_per_offset = read_write_uint::calc_max_u64s(max_bits_per_offset);
    // We need to be slightly more conservative about max_u64s_per_offset than
    // normal due to how write_short_uints is implemented.
    if max_u64s_per_offset == 1 && max_bits_per_offset > 56 {
      max_u64s_per_offset = 2;
    }

    var_metas.push(latent_meta);
    var_policies.push(LatentVarPolicy {
      table,
      encoder,
      avg_bits_per_delta,
      is_trivial,
      needs_ans,
      max_u64s_per_offset,
    });
  }

  let meta = ChunkMeta::new(mode, delta_order, var_metas);

  Ok(ChunkCompressor {
    meta,
    latent_var_policies: var_policies,
    page_infos,
    deltas,
    delta_moments,
  })
}

fn choose_delta_sample<U: UnsignedLike>(
  primary_latents: &[U],
  group_size: usize,
  n_extra_groups: usize,
) -> Vec<U> {
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
fn choose_delta_encoding_order<U: UnsignedLike>(
  primary_latents: &[U],
  unoptimized_bins_log: usize,
) -> PcoResult<usize> {
  let sample = choose_delta_sample(
    primary_latents,
    DELTA_GROUP_SIZE,
    1 + primary_latents.len() / N_PER_EXTRA_DELTA_GROUP,
  );

  let mut best_order = usize::MAX;
  let mut best_size = usize::MAX;
  for delta_encoding_order in 0..MAX_DELTA_ENCODING_ORDER + 1 {
    let sample_cc = unsigned_new_w_delta_order(
      vec![sample.clone()],
      &PagingSpec::ExactPageSizes(vec![sample.len()]),
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

fn choose_unoptimized_bins_log(compression_level: usize, n: usize) -> usize {
  let log_n = (n as f64).log2().floor() as usize;
  let fast_unoptimized_bins_log = log_n.saturating_sub(4);
  if compression_level <= fast_unoptimized_bins_log {
    compression_level
  } else {
    fast_unoptimized_bins_log + compression_level.saturating_sub(fast_unoptimized_bins_log) / 2
  }
}

// We pull this stuff out of `new` because it only depends on the unsigned type
// and we don't need a specialization for each full dtype.
fn unsigned_new<U: UnsignedLike>(
  latents: Vec<Vec<U>>,
  config: &ChunkConfig,
  mode: Mode<U>,
) -> PcoResult<ChunkCompressor<U>> {
  let unoptimized_bins_log =
    choose_unoptimized_bins_log(config.compression_level, latents[0].len());
  let delta_order = if let Some(delta_order) = config.delta_encoding_order {
    delta_order
  } else {
    choose_delta_encoding_order(&latents[0], unoptimized_bins_log)?
  };

  unsigned_new_w_delta_order(
    latents,
    &config.paging_spec,
    mode,
    delta_order,
    unoptimized_bins_log,
  )
}

// Should this take nums as a slice of slices instead of having a config.paging_spec?
pub(crate) fn new<T: NumberLike>(
  nums: &[T],
  config: &ChunkConfig,
) -> PcoResult<ChunkCompressor<T::Unsigned>> {
  validate_config(config)?;
  let n = nums.len();
  validate_chunk_size(n)?;

  let mode = choose_mode(nums, config);
  let latents = split_latents(mode, nums);

  unsigned_new(latents, config, mode)
}

impl<U: UnsignedLike> ChunkCompressor<U> {
  fn page_moments(&self, page_idx: usize, latent_var_idx: usize) -> &DeltaMoments<U> {
    &self.delta_moments[page_idx][latent_var_idx]
  }

  /// Returns the count of numbers this chunk will contain in each page.
  pub fn n_per_page(&self) -> Vec<usize> {
    self.page_infos.iter().map(|page| page.page_n).collect()
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

  fn dissect_page(&self, page_idx: usize) -> PcoResult<DissectedPage<U>> {
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
    // TODO share logic between this and bin optimization
    let page_info = &self.page_infos[page_idx];
    let mut bit_size = 0;
    for ((var_meta, var_policy), &end_idx) in self
      .meta
      .per_latent_var
      .iter()
      .zip(&self.latent_var_policies)
      .zip(&page_info.end_idx_per_var)
    {
      let page_n_deltas = end_idx - page_info.start_idx;
      let meta_bit_size = self.meta.delta_encoding_order * U::BITS as usize
        + ANS_INTERLEAVING * var_meta.ans_size_log as usize;
      // We're probably reserving more than necessary sometimes, because
      // max_bits_per_latent is quite a loose upper bound.
      // But most datasets have multiple pages, and if we really wanted to
      // improve performance for standalone files too, we'd need a whole-file
      // compressed size estimate.
      let nums_bit_size = page_n_deltas as f64 * var_policy.avg_bits_per_delta;
      bit_size += meta_bit_size + (nums_bit_size * page_size_overestimation).ceil() as usize;
    }
    bits::ceil_div(bit_size, 8)
  }

  #[inline(never)]
  fn write_dissected_page<W: Write>(
    &self,
    dissected_page: DissectedPage<U>,
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
        );
        writer.flush()?;
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

    page_meta.write_to(ans_size_logs, &mut writer);
    writer.flush()?;

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
