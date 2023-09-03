use std::cmp::max;

use crate::ans::spec::Spec;
use crate::ans::{AnsState, Token};
use crate::constants::{Bitlen, Weight};
use crate::data_types::UnsignedLike;
use crate::errors::PcoResult;
use crate::Bin;

#[derive(Clone, Debug)]
struct TokenInfo {
  renorm_bit_cutoff: AnsState,
  min_renorm_bits: Bitlen,
  next_states: Vec<AnsState>,
}

impl TokenInfo {
  fn next_state_for(&self, x_s: AnsState) -> AnsState {
    self.next_states[x_s as usize - self.next_states.len()]
  }
}

#[derive(Clone, Debug)]
pub struct Encoder {
  token_infos: Vec<TokenInfo>,
  size_log: Bitlen,
}

impl Encoder {
  pub fn from_bins<U: UnsignedLike>(size_log: Bitlen, bins: &[Bin<U>]) -> PcoResult<Self> {
    let weights = bins.iter().map(|bin| bin.weight).collect::<Vec<_>>();
    let spec = Spec::from_weights(size_log, weights)?;
    Ok(Self::new(&spec))
  }

  pub fn new(spec: &Spec) -> Self {
    let table_size = spec.table_size();

    let mut token_infos = spec
      .token_weights
      .iter()
      .map(|&weight| {
        // e.g. If the token count is 3 and table size is 16, so the x_s values
        // are in [3, 6).
        // We find the power of 2 in this range (4), then compare its log to 16
        // to find the min renormalization bits (4 - 2 = 2).
        // Finally we choose the cutoff as 2 * 3 * 2 ^ renorm_bits = 24.
        let max_x_s = 2 * weight - 1;
        let min_renorm_bits = spec.size_log - max_x_s.ilog2() as Bitlen;
        let renorm_bit_cutoff = (2 * weight * (1 << min_renorm_bits)) as AnsState;
        TokenInfo {
          renorm_bit_cutoff,
          min_renorm_bits,
          next_states: Vec::with_capacity(weight as usize),
        }
      })
      .collect::<Vec<_>>();

    for (state_idx, &token) in spec.state_tokens.iter().enumerate() {
      token_infos[token as usize]
        .next_states
        .push((table_size + state_idx) as AnsState);
    }

    Self {
      // We choose the initial state from [table_size, 2 * table_size)
      // to be the minimum as this tends to require fewer bits to encode
      // the first token.
      token_infos,
      size_log: spec.size_log,
    }
  }

  // Returns the new state, and how many bits of the existing state to write.
  // The value of those bits may contain larger significant bits that must be
  // ignored.
  // We don't write to a BitWriter directly because ANS operates in a LIFO
  // manner. We need to write these in reverse order.
  pub fn encode(&self, state: AnsState, token: Token) -> (AnsState, Bitlen) {
    let token_info = &self.token_infos[token as usize];
    let renorm_bits = if state >= token_info.renorm_bit_cutoff {
      token_info.min_renorm_bits + 1
    } else {
      token_info.min_renorm_bits
    };
    (token_info.next_state_for(state >> renorm_bits), renorm_bits)
  }

  pub fn size_log(&self) -> Bitlen {
    self.size_log
  }

  pub fn default_state(&self) -> AnsState {
    1 << self.size_log
  }
}

// given size_log, quantize the counts
fn quantize_weights_to(counts: &[Weight], total_count: usize, size_log: Bitlen) -> Vec<Weight> {
  if size_log == 0 {
    return vec![1];
  }

  let target_weight_sum = 1 << size_log;
  let multiplier = target_weight_sum as f32 / total_count as f32;
  let surplus_idxs = counts
    .iter()
    .enumerate()
    .filter_map(|(i, &count)| {
      if count as f32 * multiplier > 1.0 {
        Some(i)
      } else {
        None
      }
    })
    .collect::<Vec<_>>();
  let mut surplus = vec![0.0; counts.len()];
  let mut total_surplus = 0.0;
  for idx in surplus_idxs {
    surplus[idx] = counts[idx] as f32 * multiplier - 1.0;
    total_surplus += surplus[idx];
  }
  let target_surplus = target_weight_sum - counts.len() as Weight;
  let surplus_mult = target_surplus as f32 / total_surplus;

  let mut float_weights = vec![1.0; counts.len()];
  for idx in 0..counts.len() {
    float_weights[idx] = 1.0 + (surplus[idx] * surplus_mult);
  }

  let mut weights = float_weights
    .iter()
    .map(|&weight| weight.round() as Weight)
    .collect::<Vec<_>>();
  let mut weight_sum = weights.iter().sum::<Weight>();

  let mut i = 0;
  while weight_sum > target_weight_sum {
    if weights[i] > 1 && weights[i] as f32 > float_weights[i] {
      weights[i] -= 1;
      weight_sum -= 1;
    }
    i += 1;
  }
  i = 0;
  while weight_sum < target_weight_sum {
    if (weights[i] as f32) < float_weights[i] {
      weights[i] += 1;
      weight_sum += 1;
    }
    i += 1;
  }

  weights
}

// choose both size_log and weights
pub fn quantize_weights(
  counts: Vec<Weight>,
  total_count: usize,
  max_size_log: Bitlen,
) -> (Bitlen, Vec<Weight>) {
  if counts.len() == 1 {
    return (0, vec![1]);
  }

  let min_size_log = (usize::BITS - (counts.len() - 1).leading_zeros()) as Bitlen;
  let mut size_log = max(min_size_log, max_size_log);
  let mut weights = quantize_weights_to(&counts, total_count, size_log);

  let power_of_2 = weights.iter().map(|&w| w.trailing_zeros()).min().unwrap() as Bitlen;
  size_log -= power_of_2;
  for weight in &mut weights {
    *weight >>= power_of_2;
  }
  (size_log, weights)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_quantize_weights_to() {
    let quantized = quantize_weights_to(&[777], 777, 0);
    assert_eq!(quantized, vec![1]);

    let quantized = quantize_weights_to(&[777, 1], 778, 1);
    assert_eq!(quantized, vec![1, 1]);

    let quantized = quantize_weights_to(&[777, 1], 778, 2);
    assert_eq!(quantized, vec![3, 1]);

    let quantized = quantize_weights_to(&[2, 3, 6, 5, 1], 17, 3);
    assert_eq!(quantized, vec![1, 1, 3, 2, 1]);
  }

  #[test]
  fn test_quantize_weights() {
    let quantized = quantize_weights(vec![77, 100], 177, 4);
    assert_eq!(quantized, (4, vec![7, 9]));

    let quantized = quantize_weights(vec![77, 77], 154, 4);
    assert_eq!(quantized, (1, vec![1, 1]));
  }
}
