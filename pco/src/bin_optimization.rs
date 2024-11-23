use crate::ans::Symbol;
use crate::bits;
use crate::compression_intermediates::BinCompressionInfo;
use crate::constants::{Bitlen, Weight};
use crate::data_types::Latent;
use crate::histograms::HistogramBin;
use crate::metadata::Bin;

// vec of [start_bin_idx, end_bin_idx], inclusive
type Partitioning = Vec<(usize, usize)>;

const SINGLE_BIN_SPEEDUP_WORTH_IN_BITS_PER_NUM: f32 = 0.1;
const TRIVIAL_OFFSET_SPEEDUP_WORTH_IN_BITS_PER_NUM: f32 = 0.1;

// using f32 instead of f64 because the .log2() is faster
fn bin_cost<L: Latent>(
  bin_meta_cost: f32,
  lower: L,
  upper: L,
  count: Weight,
  total_count_log2: f32,
) -> f32 {
  let count = count as f32;
  // On Windows, log2() is very slow, so we use log(2.0) instead, which is
  // about 10x faster. On other platforms, we stick with log2(). See #223.
  #[cfg(target_os = "windows")]
  let ans_cost = total_count_log2 - count.log(2.0);
  #[cfg(not(target_os = "windows"))]
  let ans_cost = total_count_log2 - count.log2();
  let offset_cost = bits::bits_to_encode_offset(upper - lower) as f32;
  bin_meta_cost + (ans_cost + offset_cost) * count
}

fn calc_trivial_offset_partitioning<L: Latent>(
  bin_meta_cost: f32,
  total_count_log2: f32,
  bins: &[HistogramBin<L>],
) -> Option<(Partitioning, f32)> {
  if bins.iter().any(|bin| bin.lower != bin.upper) {
    return None;
  }

  let partitioning: Vec<_> = (0..bins.len()).map(|i| (i, i)).collect();
  let cost = bins
    .iter()
    .map(|bin| {
      bin_cost(
        bin_meta_cost,
        bin.lower,
        bin.upper,
        bin.count as Weight,
        total_count_log2,
      )
    })
    .sum();
  Some((partitioning, cost))
}

// Combines consecutive unoptimized bins and returns a vec of (j, i) where
// j and i are the inclusive indices of a group of bins to combine together.
// This algorithm is exactly optimal, assuming our cost estimates (measured in
// total bit size) are correct.
fn choose_optimized_partitioning<L: Latent>(
  bins: &[HistogramBin<L>],
  ans_size_log: Bitlen,
) -> Partitioning {
  let mut c = 0;
  let mut c_counts_and_best_costs = Vec::with_capacity(bins.len() + 1);
  // To keep improve performance a bit, we put cumulative count and best cost
  // into the same Vec. This frees up registers, requiring one fewer load in
  // the hot loop, at least on ARM.
  c_counts_and_best_costs.push((0, 0.0));
  for bin in bins {
    c += bin.count as u32;
    c_counts_and_best_costs.push((c, f32::NAN));
  }
  let total_count = c;
  let lowers = bins.iter().map(|bin| bin.lower).collect::<Vec<_>>();
  let uppers = bins.iter().map(|bin| bin.upper).collect::<Vec<_>>();
  let total_count_log2 = (c as f32).log2();

  let mut best_partitionings = Vec::with_capacity(bins.len() + 1);
  best_partitionings.push(Vec::new());

  let bin_meta_cost = Bin::<L>::exact_bit_size(ans_size_log) as f32;

  for i in 0..bins.len() {
    let mut best_cost = f32::MAX;
    let mut best_j = usize::MAX;
    let upper = uppers[i];
    let (c_count_i, _) = c_counts_and_best_costs[i + 1];
    for j in (0..i + 1).rev() {
      let lower = lowers[j];
      let (c_count_j, best_cost_up_to_j) = c_counts_and_best_costs[j];

      let cost = best_cost_up_to_j
        + bin_cost::<L>(
          bin_meta_cost,
          lower,
          upper,
          c_count_i - c_count_j,
          total_count_log2,
        );
      if cost < best_cost {
        best_cost = cost;
        best_j = j;
      }
    }

    c_counts_and_best_costs[i + 1].1 = best_cost;
    let mut best_partitioning = Vec::with_capacity(best_partitionings[best_j].len() + 1);
    best_partitioning.extend(&best_partitionings[best_j]);
    best_partitioning.push((best_j, i));
    best_partitionings.push(best_partitioning);
  }
  let &(_, best_cost) = c_counts_and_best_costs.last().unwrap();
  let best_partitioning = best_partitionings.last().unwrap().clone();

  let single_bin_partitioning = vec![(0_usize, bins.len() - 1)];
  let single_bin_cost = bin_cost(
    bin_meta_cost,
    lowers[0],
    uppers[bins.len() - 1],
    total_count,
    total_count_log2,
  );
  if single_bin_cost < best_cost + SINGLE_BIN_SPEEDUP_WORTH_IN_BITS_PER_NUM * total_count as f32 {
    return single_bin_partitioning;
  }

  if let Some((trivial_offset_partitioning, trivial_offset_cost)) =
    calc_trivial_offset_partitioning(bin_meta_cost, total_count_log2, bins)
  {
    if trivial_offset_cost
      < best_cost + TRIVIAL_OFFSET_SPEEDUP_WORTH_IN_BITS_PER_NUM * total_count as f32
    {
      return trivial_offset_partitioning;
    }
  }

  best_partitioning
}

pub fn optimize_bins<L: Latent>(
  bins: &[HistogramBin<L>],
  ans_size_log: Bitlen,
) -> Vec<BinCompressionInfo<L>> {
  let partitioning = choose_optimized_partitioning(bins, ans_size_log);
  let mut res = Vec::with_capacity(partitioning.len());
  for (symbol, &(j, i)) in partitioning.iter().enumerate() {
    let count: usize = bins.iter().take(i + 1).skip(j).map(|bin| bin.count).sum();
    let optimized_bin = BinCompressionInfo {
      weight: count as Weight,
      lower: bins[j].lower,
      upper: bins[i].upper,
      symbol: symbol as Symbol,
      offset_bits: bits::bits_to_encode_offset(bins[i].upper - bins[j].lower),
    };
    res.push(optimized_bin);
  }
  res
}

#[cfg(test)]
mod tests {
  use crate::bin_optimization::optimize_bins;
  use crate::compression_intermediates::BinCompressionInfo;
  use crate::histograms::HistogramBin;

  fn make_bin(count: usize, lower: u32, upper: u32) -> HistogramBin<u32> {
    HistogramBin {
      count,
      lower,
      upper,
    }
  }

  #[test]
  fn test_bin_optimization() {
    let infos = vec![
      make_bin(100, 1, 16),  // far enough from the others to stay independent
      make_bin(100, 33, 48), // same density as next bin, gets combined
      make_bin(100, 49, 64),
      make_bin(100, 65, 74), // same density as next bin (but different from previous ones)
      make_bin(50, 75, 79),
    ];
    let optimized = optimize_bins(&infos, 10);
    assert_eq!(
      optimized,
      vec![
        BinCompressionInfo {
          weight: 100,
          lower: 1,
          upper: 16,
          offset_bits: 4,
          symbol: 0,
        },
        BinCompressionInfo {
          weight: 200,
          lower: 33,
          upper: 64,
          offset_bits: 5,
          symbol: 1,
        },
        BinCompressionInfo {
          weight: 150,
          lower: 65,
          upper: 79,
          offset_bits: 4,
          symbol: 2,
        },
      ]
    )
  }

  #[test]
  fn test_bin_optimization_enveloped() {
    // here the 2nd bin would be covered by previous bin (which takes 8 offset
    // bits), but it's disadvantageous to combine them because the 2nd bin has
    // so much higher density
    let infos = vec![make_bin(1000, 0, 150), make_bin(1000, 200, 200)];
    let optimized = optimize_bins(&infos, 10);
    assert_eq!(
      optimized,
      vec![
        BinCompressionInfo {
          weight: 1000,
          lower: 0,
          upper: 150,
          offset_bits: 8,
          symbol: 0,
        },
        BinCompressionInfo {
          weight: 1000,
          lower: 200,
          upper: 200,
          offset_bits: 0,
          symbol: 1,
        },
      ]
    )
  }
}
