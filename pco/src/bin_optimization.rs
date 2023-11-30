use crate::ans::Token;
use crate::bin::BinCompressionInfo;
use crate::bits;
use crate::bits::avg_depth_bits;
use crate::constants::{Bitlen, Weight};
use crate::data_types::UnsignedLike;

fn bin_bit_cost<U: UnsignedLike>(
  base_meta_cost: f64,
  lower: U,
  upper: U,
  count: Weight,
  n: usize,
) -> f64 {
  let ans_cost = avg_depth_bits(count, n);
  let offset_cost = (bits::bits_to_encode_offset(upper - lower) as u64 * count as u64) as f64;
  base_meta_cost + ans_cost * (count as f64) + offset_cost
}

// this is an exact optimal strategy
pub fn optimize_bins<U: UnsignedLike>(
  bins: Vec<BinCompressionInfo<U>>,
  ans_size_log: Bitlen,
  n: usize,
) -> Vec<BinCompressionInfo<U>> {
  let mut c = 0;
  let mut cum_count = Vec::with_capacity(bins.len() + 1);
  cum_count.push(0);
  for bin in &bins {
    c += bin.weight;
    cum_count.push(c);
  }
  let lowers = bins.iter().map(|bin| bin.lower).collect::<Vec<_>>();
  let uppers = bins.iter().map(|bin| bin.upper).collect::<Vec<_>>();

  let mut best_costs = Vec::with_capacity(bins.len() + 1);
  let mut best_paths = Vec::with_capacity(bins.len() + 1);
  best_costs.push(0.0);
  best_paths.push(Vec::new());

  let bits_to_encode_weight = ans_size_log;
  let base_meta_cost = bits_to_encode_weight as f64 +
    U::BITS as f64 + // lower bound
    bits::bits_to_encode_offset_bits::<U>() as f64;

  for i in 0..bins.len() {
    let mut best_cost = f64::MAX;
    let mut best_j = usize::MAX;
    let upper = uppers[i];
    let cum_count_i = cum_count[i + 1];
    for j in (0..i + 1).rev() {
      let lower = lowers[j];

      let cost = best_costs[j]
        + bin_bit_cost::<U>(
          base_meta_cost,
          lower,
          upper,
          cum_count_i - cum_count[j],
          n,
        );
      if cost < best_cost {
        best_cost = cost;
        best_j = j;
      }
    }

    best_costs.push(best_cost);
    let mut best_path = Vec::with_capacity(best_paths[best_j].len() + 1);
    best_path.extend(&best_paths[best_j]);
    best_path.push((best_j, i));
    best_paths.push(best_path);
  }

  let path = best_paths.last().unwrap();
  let mut res = Vec::with_capacity(path.len());
  for (token, &(j, i)) in path.iter().enumerate() {
    let mut count = 0;
    for bin in bins.iter().take(i + 1).skip(j).rev() {
      count += bin.weight;
    }
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
    let optimized = optimize_bins(infos, 10, 450);
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
}
