use crate::data_types::NumberLike;
use crate::prefix::WeightedPrefix;
use crate::{Flags, Prefix};
use crate::bits::{avg_offset_bits, avg_depth_bits};
use crate::constants::BITS_TO_ENCODE_N_ENTRIES;

fn prefix_bit_cost<T: NumberLike>(
  base_meta_cost: f64,
  lower: T::Unsigned,
  upper: T::Unsigned,
  weight: usize,
  total_weight: usize,
) -> f64 {
  let offset_cost = avg_offset_bits(lower, upper);
  let huffman_cost = avg_depth_bits(weight, total_weight);
  base_meta_cost +
    huffman_cost + // extra meta cost depending on length of huffman code
    (offset_cost + huffman_cost) * weight as f64 // body cost
}

// this is an exact optimal strategy
pub fn optimize_prefixes<T: NumberLike>(
  wprefixes: Vec<WeightedPrefix<T>>,
  flags: &Flags,
) -> Vec<WeightedPrefix<T>> {
  let mut c = 0;
  let mut cum_weight = Vec::with_capacity(wprefixes.len() + 1);
  cum_weight.push(0);
  for wp in &wprefixes {
    c += wp.weight;
    cum_weight.push(c);
  }
  let total_weight = cum_weight[cum_weight.len() - 1];
  let lower_unsigneds = wprefixes.iter()
    .map(|wp| wp.prefix.lower.to_unsigned())
    .collect::<Vec<_>>();

  let maybe_rep_ind = wprefixes.iter()
    .position(|wp| wp.prefix.run_len_jumpstart.is_some());

  let mut best_costs = Vec::with_capacity(wprefixes.len() + 1);
  let mut best_paths = Vec::with_capacity(wprefixes.len() + 1);
  best_costs.push(0.0);
  best_paths.push(Vec::new());

  let base_meta_cost = BITS_TO_ENCODE_N_ENTRIES as f64 +
    2.0 * T::PHYSICAL_BITS as f64 + // lower and upper bounds
    flags.bits_to_encode_prefix_len() as f64 +
    1.0; // bit to say there is no run len jumpstart
  for i in 0..wprefixes.len() {
    let mut best_cost = f64::MAX;
    let mut best_j = usize::MAX;
    let upper = wprefixes[i].prefix.upper.to_unsigned();
    let cum_weight_i = cum_weight[i + 1];
    let start_j = match maybe_rep_ind {
      Some(ind) if ind < i => ind + 1,
      Some(ind) if ind == i => ind,
      _ => 0,
    };
    for j in start_j..i + 1 {
      let cost = best_costs[j] + prefix_bit_cost::<T>(
        base_meta_cost,
        lower_unsigneds[j],
        upper,
        cum_weight_i - cum_weight[j],
        total_weight,
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
    for wp in wprefixes.iter().take(i + 1).skip(j) {
      count += wp.prefix.count;
    }
    let prefix = Prefix {
      count,
      code: Vec::new(),
      lower: wprefixes[j].prefix.lower,
      upper: wprefixes[i].prefix.upper,
      run_len_jumpstart: wprefixes[i].prefix.run_len_jumpstart,
    };
    res.push(WeightedPrefix {
      weight: cum_weight[i + 1] - cum_weight[j],
      prefix,
    })
  }
  res
}

