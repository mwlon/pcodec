use crate::{bits, Flags, run_len_utils};
use crate::base_compressor::InternalCompressorConfig;
use crate::bin::BinCompressionInfo;
use crate::bits::{avg_depth_bits, avg_offset_bits};
use crate::constants::BITS_TO_ENCODE_CODE_LEN;
use crate::data_types::UnsignedLike;
use crate::modes::{gcd, Mode};

fn bin_bit_cost<U: UnsignedLike, M: Mode<U>>(
  base_meta_cost: f64,
  lower: U,
  upper: U,
  count: usize,
  n: usize,
  mode: M,
  acc: &M::BinOptAccumulator,
) -> f64 {
  let (weight, jumpstart_cost) = run_len_utils::weight_and_jumpstart_cost(count, n);
  let total_weight = n + weight - count;
  let huffman_cost = avg_depth_bits(weight, total_weight);
  let mode_cost = mode.bin_cost(lower, upper, count, acc);
  base_meta_cost + huffman_cost + // extra meta cost of storing Huffman code
    (huffman_cost + jumpstart_cost) * (weight as f64) +
    mode_cost
}

// this is an exact optimal strategy
pub fn optimize_bins<U: UnsignedLike, M: Mode<U>>(
  bins: Vec<BinCompressionInfo<U>>,
  flags: &Flags,
  mode: M,
  n: usize,
) -> Vec<BinCompressionInfo<U>> {
  let mut c = 0;
  let mut cum_count = Vec::with_capacity(bins.len() + 1);
  cum_count.push(0);
  for bin in &bins {
    c += bin.count;
    cum_count.push(c);
  }
  let lowers = bins.iter().map(|bin| bin.lower).collect::<Vec<_>>();
  let uppers = bins.iter().map(|bin| bin.upper).collect::<Vec<_>>();

  let mut best_costs = Vec::with_capacity(bins.len() + 1);
  let mut best_paths = Vec::with_capacity(bins.len() + 1);
  best_costs.push(0.0);
  best_paths.push(Vec::new());

  let bits_to_encode_count = flags.bits_to_encode_count(n);
  let base_meta_cost = bits_to_encode_count as f64 +
    U::BITS as f64 + // lower and upper bounds
    bits::bits_to_encode_offset_bits::<U>() as f64 +
    BITS_TO_ENCODE_CODE_LEN as f64 +
    M::EXTRA_META_COST +
    1.0; // bit to say there is no run len jumpstart

  for i in 0..bins.len() {
    let mut best_cost = f64::MAX;
    let mut best_j = usize::MAX;
    let mut acc = M::BinOptAccumulator::default();
    let upper = uppers[i];
    let cum_count_i = cum_count[i + 1];
    for j in (0..i + 1).rev() {
      let lower = lowers[j];

      M::combine_bin_opt_acc(&bins[j], &mut acc);
      let cost = best_costs[j] +
        bin_bit_cost::<U, M>(
          base_meta_cost,
          lower,
          upper,
          cum_count_i - cum_count[j],
          n,
          mode,
          &acc,
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
    let mut acc = M::BinOptAccumulator::default();
    for bin in bins.iter().take(i + 1).skip(j).rev() {
      count += bin.count;
      M::combine_bin_opt_acc(bin, &mut acc);
    }
    let mut optimized_bin = BinCompressionInfo {
      count,
      lower: bins[j].lower,
      upper: bins[i].upper,
      run_len_jumpstart: run_len_utils::run_len_jumpstart(count, n),
      ..Default::default()
    };
    mode.fill_optimized_compression_info(
      acc,
      &mut optimized_bin,
    );
    res.push(optimized_bin);
  }
  res
}

#[cfg(test)]
mod tests {
  use crate::{bits, CompressorConfig, Flags};
  use crate::base_compressor::InternalCompressorConfig;
  use crate::bin::BinCompressionInfo;
  use crate::bin_optimization::optimize_bins;
  use crate::constants::Bitlen;
  use crate::modes::gcd::GcdMode;

  fn basic_gcd_optimize(bins: Vec<BinCompressionInfo<u32>>) -> Vec<BinCompressionInfo<u32>> {
    let flags = Flags {
      delta_encoding_order: 0,
      use_wrapped_mode: false,
    };
    optimize_bins(bins, &flags, GcdMode, 100)
  }

  fn make_gcd_bin_info(count: usize, lower: u32, upper: u32, gcd: u32) -> BinCompressionInfo<u32> {
    let offset_bits = bits::bits_to_encode_offset((upper - lower) / gcd);
    BinCompressionInfo {
      count,
      offset_bits,
      lower,
      upper,
      gcd,
      ..Default::default()
    }
  }

  #[test]
  fn test_optimize_trivial_ranges_gcd() {
    let bins = vec![
      make_gcd_bin_info(1, 1000_u32, 1000, 1_u32),
      make_gcd_bin_info(1, 2000_u32, 2000, 1_u32),
    ];
    let res = basic_gcd_optimize(bins);
    let expected = vec![make_gcd_bin_info(2, 1000_u32, 2000, 1000_u32)];
    assert_eq!(res, expected);
  }

  #[test]
  fn test_optimize_single_nontrivial_range_gcd() {
    let bins = vec![
      make_gcd_bin_info(100, 1000_u32, 2000, 10_u32),
      make_gcd_bin_info(1, 2100_u32, 2100, 1_u32),
    ];
    let res = basic_gcd_optimize(bins);
    let expected = vec![make_gcd_bin_info(101, 1000_u32, 2100, 10_u32)];
    assert_eq!(res, expected);
  }

  #[test]
  fn test_optimize_nontrivial_ranges_gcd() {
    let bins = vec![
      make_gcd_bin_info(5, 1000_u32, 1100, 10_u32),
      make_gcd_bin_info(5, 1105, 1135, 15_u32),
    ];
    let res = basic_gcd_optimize(bins);
    let expected = vec![make_gcd_bin_info(10, 1000_u32, 1135, 5_u32)];
    assert_eq!(res, expected);
  }

  #[test]
  fn test_optimize_nontrivial_misaligned_ranges_gcd() {
    let bins = vec![
      make_gcd_bin_info(100, 1000_u32, 1100, 10_u32),
      make_gcd_bin_info(100, 1101, 1201, 10_u32),
    ];
    let res = basic_gcd_optimize(bins);
    let expected = vec![
      make_gcd_bin_info(100, 1000_u32, 1100, 10_u32),
      make_gcd_bin_info(100, 1101, 1201, 10_u32),
    ];
    assert_eq!(res, expected);
  }
}
