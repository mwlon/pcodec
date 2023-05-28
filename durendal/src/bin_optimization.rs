use crate::base_compressor::InternalCompressorConfig;
use crate::bits::{avg_depth_bits, avg_offset_bits};
use crate::constants::BITS_TO_ENCODE_CODE_LEN;
use crate::data_types::UnsignedLike;
use crate::modes::gcd;
use crate::{bits, Flags, run_len_utils};
use crate::bin::BinCompressionInfo;

fn bin_bit_cost<U: UnsignedLike>(
  base_meta_cost: f64,
  lower: U,
  upper: U,
  count: usize,
  n: usize,
  gcd: U,
) -> f64 {
  let offset_cost = avg_offset_bits(lower, upper, gcd);
  let (weight, jumpstart_cost) = run_len_utils::weight_and_jumpstart_cost(count, n);
  let total_weight = n + weight - count;
  let huffman_cost = avg_depth_bits(weight, total_weight);
  // best approximation of GCD metadata bit cost we can do without knowing
  // what's going on in the other bins
  let gcd_cost = if gcd > U::ONE { U::BITS as f64 } else { 0.0 };
  base_meta_cost +
    gcd_cost + // extra meta cost of storing GCD
    huffman_cost + // extra meta cost of storing Huffman code
    (huffman_cost + jumpstart_cost) * (weight as f64) +
    offset_cost * count as f64
}

// this is an exact optimal strategy
pub fn optimize_bins<U: UnsignedLike>(
  bins: Vec<BinCompressionInfo<U>>,
  internal_config: &InternalCompressorConfig,
  flags: &Flags,
  n: usize,
) -> Vec<BinCompressionInfo<U>> {
  let mut c = 0;
  let mut cum_count = Vec::with_capacity(bins.len() + 1);
  cum_count.push(0);
  for bin in &bins {
    c += bin.count;
    cum_count.push(c);
  }
  let gcds = bins.iter().map(|p| p.gcd).collect::<Vec<_>>();
  let lower_unsigneds = bins.iter().map(|p| p.lower).collect::<Vec<_>>();
  let upper_unsigneds = bins.iter().map(|bin| bin.upper).collect::<Vec<_>>();

  let mut best_costs = Vec::with_capacity(bins.len() + 1);
  let mut best_paths = Vec::with_capacity(bins.len() + 1);
  best_costs.push(0.0);
  best_paths.push(Vec::new());

  let bits_to_encode_count = flags.bits_to_encode_count(n);
  let base_meta_cost = bits_to_encode_count as f64 +
    U::BITS as f64 + // lower and upper bounds
    bits::bits_to_encode_offset_bits::<U>() as f64 +
    BITS_TO_ENCODE_CODE_LEN as f64 +
    1.0; // bit to say there is no run len jumpstart

  // determine whether we can skip GCD folding to improve performance in some cases
  let fold_gcd = gcd::use_gcd_bin_optimize(&bins, internal_config);

  for i in 0..bins.len() {
    let mut best_cost = f64::MAX;
    let mut best_j = usize::MAX;
    let upper = upper_unsigneds[i];
    let mut gcd_acc = None;
    let cum_count_i = cum_count[i + 1];
    for j in (0..i + 1).rev() {
      let lower = lower_unsigneds[j];
      if fold_gcd {
        gcd::fold_bin_gcds_left(
          lower,
          upper_unsigneds[j],
          gcds[j],
          upper,
          &mut gcd_acc,
        );
      }
      let cost = best_costs[j]
        + bin_bit_cost::<U>(
        base_meta_cost,
        lower,
        upper,
        cum_count_i - cum_count[j],
        n,
        gcd_acc.unwrap_or(U::ONE),
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
  for &(j, i) in path {
    let mut count = 0;
    let mut gcd_acc = None;
    for (k, p) in bins.iter().enumerate().take(i + 1).skip(j).rev() {
      count += p.count;
      if fold_gcd {
        gcd::fold_bin_gcds_left(
          lower_unsigneds[k],
          upper_unsigneds[k],
          p.gcd,
          upper_unsigneds[i],
          &mut gcd_acc,
        );
      }
    }
    res.push(BinCompressionInfo::new(
      count,
      bins[j].lower,
      bins[i].upper,
      run_len_utils::run_len_jumpstart(count, n),
      gcd_acc.unwrap_or(U::ONE),
    ));
  }
  res
}

#[cfg(test)]
mod tests {
  use crate::base_compressor::InternalCompressorConfig;
  use crate::bin::{BinCompressionInfo};
  use crate::bin_optimization::optimize_bins;
  use crate::Flags;

  fn basic_optimize(wps: Vec<BinCompressionInfo<u32>>) -> Vec<BinCompressionInfo<u32>> {
    let flags = Flags {
      delta_encoding_order: 0,
      use_wrapped_mode: false,
    };
    let internal_config = InternalCompressorConfig {
      compression_level: 6,
      use_gcds: true,
    };
    optimize_bins(wps, &internal_config, &flags, 100)
  }

  #[test]
  fn test_optimize_trivial_ranges_gcd() {
    let bins = vec![
      BinCompressionInfo::new(1, 1000_u32, 1000, None, 1_u32),
      BinCompressionInfo::new(1, 2000_u32, 2000, None, 1_u32),
    ];
    let res = basic_optimize(bins);
    let expected = vec![BinCompressionInfo::new(2, 1000_u32, 2000, None, 1000_u32)];
    assert_eq!(res, expected);
  }

  #[test]
  fn test_optimize_single_nontrivial_range_gcd() {
    let bins = vec![
      BinCompressionInfo::new(100, 1000_u32, 2000, None, 10_u32),
      BinCompressionInfo::new(1, 2100_u32, 2100, None, 1_u32),
    ];
    let res = basic_optimize(bins);
    let expected = vec![BinCompressionInfo::new(101, 1000_u32, 2100, None, 10_u32)];
    assert_eq!(res, expected);
  }

  #[test]
  fn test_optimize_nontrivial_ranges_gcd() {
    let bins = vec![
      BinCompressionInfo::new(5, 1000_u32, 1100, None, 10_u32),
      BinCompressionInfo::new(5, 1105, 1135, None, 15_u32),
    ];
    let res = basic_optimize(bins);
    let expected = vec![BinCompressionInfo::new(10, 1000_u32, 1135, None, 5_u32)];
    assert_eq!(res, expected);
  }

  #[test]
  fn test_optimize_nontrivial_misaligned_ranges_gcd() {
    let bins = vec![
      BinCompressionInfo::new(100, 1000_u32, 1100, None, 10_u32),
      BinCompressionInfo::new(100, 1101, 1201, None, 10_u32),
    ];
    let res = basic_optimize(bins);
    let expected = vec![
      BinCompressionInfo::new(100, 1000_u32, 1100, None, 10_u32),
      BinCompressionInfo::new(100, 1101, 1201, None, 10_u32),
    ];
    assert_eq!(res, expected);
  }
}
