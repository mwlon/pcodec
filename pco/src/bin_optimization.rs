use crate::ans::Token;
use crate::bin::BinCompressionInfo;
use crate::constants::{Bitlen, Weight};
use crate::data_types::Latent;
use crate::{bits, chunk_meta};

const SINGLE_BIN_SPEEDUP_WORTH_IN_BITS_PER_NUM: f32 = 0.1;

// using f32 instead of f64 because the .log2() is faster
fn bin_cost<L: Latent>(
  bin_meta_cost: f32,
  lower: L,
  upper: L,
  count: Weight,
  total_count_log2: f32,
) -> f32 {
  let count = count as f32;
  let ans_cost = total_count_log2 - count.log2();
  let offset_cost = bits::bits_to_encode_offset(upper - lower) as f32;
  bin_meta_cost + (ans_cost + offset_cost) * count
}

// Combines consecutive unoptimized bins and returns a vec of (j, i) where
// j and i are the inclusive indices of a group of bins to combine together.
// This algorithm is exactly optimal, assuming our cost estimates (measured in
// total bit size) are correct.
fn choose_optimized_partitioning<L: Latent>(
  bins: &[BinCompressionInfo<L>],
  ans_size_log: Bitlen,
) -> Vec<(usize, usize)> {
  let mut c = 0;
  let mut cum_count = Vec::with_capacity(bins.len() + 1);
  cum_count.push(0);
  for bin in bins {
    c += bin.weight;
    cum_count.push(c);
  }
  let total_count = c;
  let lowers = bins.iter().map(|bin| bin.lower).collect::<Vec<_>>();
  let uppers = bins.iter().map(|bin| bin.upper).collect::<Vec<_>>();
  let total_count_log2 = (c as f32).log2();

  let mut best_costs = Vec::with_capacity(bins.len() + 1);
  let mut best_partitionings = Vec::with_capacity(bins.len() + 1);
  best_costs.push(0.0);
  best_partitionings.push(Vec::new());

  let bin_meta_cost = chunk_meta::bin_exact_bit_size::<L>(ans_size_log) as f32;

  for i in 0..bins.len() {
    let mut best_cost = f32::MAX;
    let mut best_j = usize::MAX;
    let upper = uppers[i];
    let cum_count_i = cum_count[i + 1];
    for j in (0..i + 1).rev() {
      let lower = lowers[j];

      let cost = best_costs[j]
        + bin_cost::<L>(
          bin_meta_cost,
          lower,
          upper,
          cum_count_i - cum_count[j],
          total_count_log2,
        );
      if cost < best_cost {
        best_cost = cost;
        best_j = j;
      }
    }

    best_costs.push(best_cost);
    let mut best_partitioning = Vec::with_capacity(best_partitionings[best_j].len() + 1);
    best_partitioning.extend(&best_partitionings[best_j]);
    best_partitioning.push((best_j, i));
    best_partitionings.push(best_partitioning);
  }

  let single_bin_partitioning = vec![(0_usize, bins.len() - 1)];
  let single_bin_cost = bin_cost(
    bin_meta_cost,
    lowers[0],
    uppers[bins.len() - 1],
    total_count,
    total_count_log2,
  );
  let best_cost = best_costs.last().unwrap();
  if single_bin_cost < best_cost + SINGLE_BIN_SPEEDUP_WORTH_IN_BITS_PER_NUM * total_count as f32 {
    single_bin_partitioning
  } else {
    best_partitionings.last().unwrap().clone()
  }
}

pub fn optimize_bins<L: Latent>(
  bins: &[BinCompressionInfo<L>],
  ans_size_log: Bitlen,
) -> Vec<BinCompressionInfo<L>> {
  let partitioning = choose_optimized_partitioning(bins, ans_size_log);
  let mut res = Vec::with_capacity(partitioning.len());
  for (token, &(j, i)) in partitioning.iter().enumerate() {
    let count = bins.iter().take(i + 1).skip(j).map(|bin| bin.weight).sum();
    let optimized_bin = BinCompressionInfo {
      weight: count,
      lower: bins[j].lower,
      upper: bins[i].upper,
      token: token as Token,
      offset_bits: bits::bits_to_encode_offset(bins[i].upper - bins[j].lower),
    };
    res.push(optimized_bin);
  }
  res
}

#[cfg(test)]
mod tests {
  use crate::bin::BinCompressionInfo;
  use crate::bin_optimization::optimize_bins;
  use crate::constants::Weight;

  fn make_info(weight: Weight, lower: u32, upper: u32) -> BinCompressionInfo<u32> {
    BinCompressionInfo {
      weight,
      lower,
      upper,
      offset_bits: 0, // not used
      token: 0,       // not used
    }
  }

  #[test]
  fn test_bin_optimization() {
    let infos = vec![
      make_info(100, 1, 16),  // far enough from the others to stay independent
      make_info(100, 33, 48), // same density as next bin, gets combined
      make_info(100, 49, 64),
      make_info(100, 65, 74), // same density as next bin (but different from previous ones)
      make_info(50, 75, 79),
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
          token: 0,
        },
        BinCompressionInfo {
          weight: 200,
          lower: 33,
          upper: 64,
          offset_bits: 5,
          token: 1,
        },
        BinCompressionInfo {
          weight: 150,
          lower: 65,
          upper: 79,
          offset_bits: 4,
          token: 2,
        },
      ]
    )
  }

  #[test]
  fn test_bin_optimization_enveloped() {
    // here the 2nd bin would be covered by previous bin (which takes 8 offset
    // bits), but it's disadvantageous to combine them because the 2nd bin has
    // so much higher density
    let infos = vec![make_info(1000, 0, 150), make_info(1000, 200, 200)];
    let optimized = optimize_bins(&infos, 10);
    assert_eq!(
      optimized,
      vec![
        BinCompressionInfo {
          weight: 1000,
          lower: 0,
          upper: 150,
          offset_bits: 8,
          token: 0,
        },
        BinCompressionInfo {
          weight: 1000,
          lower: 200,
          upper: 200,
          offset_bits: 0,
          token: 1,
        },
      ]
    )
  }
}
