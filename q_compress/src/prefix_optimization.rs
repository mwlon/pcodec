use std::marker::PhantomData;

use crate::{Flags, gcd_utils, Prefix};
use crate::bits::{avg_depth_bits, avg_offset_bits};
use crate::data_types::{NumberLike, UnsignedLike};
use crate::prefix::WeightedPrefix;

fn prefix_bit_cost<U: UnsignedLike>(
  base_meta_cost: f64,
  lower: U,
  upper: U,
  weight: usize,
  total_weight: usize,
  gcd: U,
) -> f64 {
  let offset_cost = avg_offset_bits(lower, upper, gcd);
  let huffman_cost = avg_depth_bits(weight, total_weight);
  let gcd_cost = if gcd > U::ONE {
    gcd_utils::gcd_bits_required(upper - lower) as f64
  } else {
    0.0
  };
  base_meta_cost +
    gcd_cost + // extra meta cost of storing GCD
    huffman_cost + // extra meta cost of storing Huffman code
    (offset_cost + huffman_cost) * weight as f64 // body cost
}

// this is an exact optimal strategy
pub fn optimize_prefixes<T: NumberLike>(
  wprefixes: Vec<WeightedPrefix<T>>,
  flags: &Flags,
  n: usize,
) -> Vec<WeightedPrefix<T>> {
  let mut c = 0;
  let mut cum_weight = Vec::with_capacity(wprefixes.len() + 1);
  cum_weight.push(0);
  for wp in &wprefixes {
    c += wp.weight;
    cum_weight.push(c);
  }
  let prefixes = wprefixes.iter()
    .map(|wp| wp.prefix.clone())
    .collect::<Vec<_>>();
  let gcds = prefixes.iter()
    .map(|p| p.gcd)
    .collect::<Vec<_>>();
  let total_weight = c;
  let lower_unsigneds = prefixes.iter()
    .map(|p| p.lower.to_unsigned())
    .collect::<Vec<_>>();
  let upper_unsigneds = prefixes.iter()
    .map(|p| p.upper.to_unsigned())
    .collect::<Vec<_>>();

  let maybe_rep_idx = prefixes.iter()
    .position(|p| p.run_len_jumpstart.is_some());

  let mut best_costs = Vec::with_capacity(wprefixes.len() + 1);
  let mut best_paths = Vec::with_capacity(wprefixes.len() + 1);
  best_costs.push(0.0);
  best_paths.push(Vec::new());

  let bits_to_encode_count = flags.bits_to_encode_count(n);
  let base_meta_cost = bits_to_encode_count as f64 +
    2.0 * T::PHYSICAL_BITS as f64 + // lower and upper bounds
    flags.bits_to_encode_code_len() as f64 +
    if flags.use_gcds { 1.0 } else { 0.0 } + // bit to say whether there is GCD or not
    1.0; // bit to say there is no run len jumpstart
  // determine whether we can skip GCD folding to improve performance in some cases
  let fold_gcd = gcd_utils::use_gcd_prefix_optimize(&prefixes, flags);

  for i in 0..wprefixes.len() {
    let mut best_cost = f64::MAX;
    let mut best_j = usize::MAX;
    let upper = upper_unsigneds[i];
    let cum_weight_i = cum_weight[i + 1];
    let start_j = match maybe_rep_idx {
      Some(ind) if ind < i => ind + 1,
      Some(ind) if ind == i => ind,
      _ => 0,
    };
    let mut gcd_acc = None;
    for j in (start_j..i + 1).rev() {
      let lower = lower_unsigneds[j];
      if fold_gcd {
        gcd_utils::fold_prefix_gcds_left(
          lower,
          upper_unsigneds[j],
          gcds[j],
          upper,
          &mut gcd_acc
        );
      }
      let cost = best_costs[j] + prefix_bit_cost::<T::Unsigned>(
        base_meta_cost,
        lower,
        upper,
        cum_weight_i - cum_weight[j],
        total_weight,
        gcd_acc.unwrap_or(T::Unsigned::ONE),
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
    for (k, p) in prefixes.iter().enumerate().take(i + 1).skip(j).rev() {
      count += p.count;
      if fold_gcd {
        gcd_utils::fold_prefix_gcds_left(
          lower_unsigneds[k],
          upper_unsigneds[k],
          p.gcd,
          upper_unsigneds[i],
          &mut gcd_acc,
        );
      }
    }
    let prefix = Prefix {
      count,
      code: Vec::new(),
      lower: prefixes[j].lower,
      upper: prefixes[i].upper,
      run_len_jumpstart: prefixes[i].run_len_jumpstart,
      gcd: gcd_acc.unwrap_or(T::Unsigned::ONE),
      phantom: PhantomData,
    };
    res.push(WeightedPrefix {
      weight: cum_weight[i + 1] - cum_weight[j],
      prefix,
    })
  }
  res
}

#[cfg(test)]
mod tests {
  use std::marker::PhantomData;

  use crate::Flags;
  use crate::prefix::WeightedPrefix;
  use crate::prefix_optimization::optimize_prefixes;

  fn basic_flags() -> Flags {
    Flags {
      use_gcds: true,
      use_min_count_encoding: true,
      use_5_bit_code_len: true,
      delta_encoding_order: 0,
      phantom: PhantomData,
    }
  }

  #[test]
  fn test_optimize_trivial_ranges_gcd() {
    let wps = vec![
      WeightedPrefix::new(1, 1, 1000_i32, 1000, None, 1_u32),
      WeightedPrefix::new(1, 1, 2000_i32, 2000, None, 1_u32),
    ];
    let res = optimize_prefixes(
      wps,
      &basic_flags(),
      100,
    );
    let expected = vec![
      WeightedPrefix::new(2, 2, 1000_i32, 2000, None, 1000_u32),
    ];
    assert_eq!(res, expected);
  }

  #[test]
  fn test_optimize_single_nontrivial_range_gcd() {
    let wps = vec![
      WeightedPrefix::new(100, 100, 1000_i32, 2000, None, 10_u32),
      WeightedPrefix::new(1, 1, 2100_i32, 2100, None, 1_u32),
    ];
    let res = optimize_prefixes(
      wps,
      &basic_flags(),
      100,
    );
    let expected = vec![
      WeightedPrefix::new(101, 101, 1000_i32, 2100, None, 10_u32),
    ];
    assert_eq!(res, expected);
  }

  #[test]
  fn test_optimize_nontrivial_ranges_gcd() {
    let wps = vec![
      WeightedPrefix::new(5, 5, 1000_i32, 1100, None, 10_u32),
      WeightedPrefix::new(5, 5, 1105, 1135, None, 15_u32),
    ];
    let res = optimize_prefixes(
      wps,
      &basic_flags(),
      100,
    );
    let expected = vec![
      WeightedPrefix::new(10, 10, 1000_i32, 1135, None, 5_u32),
    ];
    assert_eq!(res, expected);
  }

  #[test]
  fn test_optimize_nontrivial_misaligned_ranges_gcd() {
    let wps = vec![
      WeightedPrefix::new(100, 100, 1000_i32, 1100, None, 10_u32),
      WeightedPrefix::new(100, 100, 1101, 1201, None, 10_u32),
    ];
    let res = optimize_prefixes(
      wps,
      &basic_flags(),
      100,
    );
    let expected = vec![
      WeightedPrefix::new(100, 100, 1000_i32, 1100, None, 10_u32),
      WeightedPrefix::new(100, 100, 1101, 1201, None, 10_u32),
    ];
    assert_eq!(res, expected);
  }
}
