use crate::compression_intermediates::PageLatents;
use crate::data_types::{NumberLike, UnsignedLike};
use crate::sampling;
use crate::wrapped::SecondaryLatents;
use rand_xoshiro::rand_core::{RngCore, SeedableRng};
use std::cmp::min;
use std::collections::HashMap;

const SMALL_INTS: [u64; 4] = [1, 2, 3, 4];

pub fn split_latents<T: NumberLike>(nums: &[T], base: T::Unsigned) -> PageLatents<T::Unsigned> {
  let mut mults = Vec::with_capacity(nums.len());
  let mut adjs = Vec::with_capacity(nums.len());
  for num in nums {
    let u = num.to_unsigned();
    mults.push(u / base);
    adjs.push(u % base);
  }
  PageLatents::new_pre_delta(vec![mults, adjs])
}

pub(crate) fn join_latents<U: UnsignedLike>(
  base: U,
  unsigneds: &mut [U],
  secondary: SecondaryLatents<U>,
) {
  match secondary {
    SecondaryLatents::Nonconstant(adjustments) => {
      for (u, &adj) in unsigneds.iter_mut().zip(adjustments.iter()) {
        *u = (*u * base).wrapping_add(adj)
      }
    }
    SecondaryLatents::Constant(adj) => {
      for u in unsigneds.iter_mut() {
        *u = (*u * base).wrapping_add(adj)
      }
    }
  }
}

fn calc_gcd<U: UnsignedLike>(mut x: U, mut y: U) -> U {
  if x == U::ZERO {
    return y;
  }

  loop {
    if y == U::ZERO {
      return x;
    }

    x %= y;
    std::mem::swap(&mut x, &mut y);
  }
}

fn shuffle_sample<U: Copy>(sample: &mut [U]) {
  let mut rng = rand_xoshiro::Xoroshiro128PlusPlus::seed_from_u64(0);
  for i in 0..sample.len() {
    let rand_idx = i + (rng.next_u64() as usize - i) % (sample.len() - i);
    sample.swap(i, rand_idx);
  }
}

fn calc_triple_gcd<U: UnsignedLike>(triple: &[U]) -> U {
  let a = triple[0];
  let b = triple[1];
  let c = triple[2];
  let (lower, x, y) = if a < b {
    if a < c {
      (a, b, c)
    } else {
      (c, a, b)
    }
  } else if b < c {
    (b, c, a)
  } else {
    (c, a, b)
  };

  calc_gcd(x - lower, y - lower)
}

fn score_triple_gcd<U: UnsignedLike>(
  gcd: U,
  triples_w_gcd: usize,
  total_triples: usize,
) -> Option<f64> {
  if triples_w_gcd <= 1 {
    // not enough to make any claims
    return None;
  }

  // defining rarity as 1 / probability
  let prob_per_triple = triples_w_gcd as f64 / total_triples as f64;
  let implied_prob_per_num = prob_per_triple.sqrt();
  let gcd_f64 = min(gcd, U::from_u64(u64::MAX)).to_u64() as f64;

  // check if the GCD has statistical evidence (3 sigma)
  let natural_prob_per_num = 1.0 / gcd_f64;
  let stdev = (natural_prob_per_num * (1.0 - natural_prob_per_num) / total_triples as f64).sqrt();
  let z_score = (implied_prob_per_num - natural_prob_per_num) / stdev;
  if z_score < 3.0 {
    return None;
  }

  // heuristic for when the GCD is useless, even if true
  if implied_prob_per_num < 0.1 || implied_prob_per_num < 1.0 / (0.9 + 0.2 * gcd_f64) {
    return None;
  }

  // heuristic for how good a GCD is. It mostly scales with overperformance of
  // the GCD relative to expectations, but that breaks down when considering
  // multiples of the GCD. e.g. if 100 is the true GCD, 200 will appear half
  // as often and look equally enticing. To decide between them we add a small
  // penalty for larger GCDs.
  let score = (implied_prob_per_num - 0.05) * gcd_f64;
  Some(score)
}

fn most_prominent_gcd<U: UnsignedLike>(triple_gcds: &[U], total_triples: usize) -> Option<U> {
  let mut raw_counts = HashMap::new();
  for &gcd in triple_gcds {
    *raw_counts.entry(gcd).or_insert(0) += 1;
  }

  let mut counts_accounting_for_small_multiples = HashMap::new();
  for (&gcd, &count) in raw_counts.iter() {
    for divisor in SMALL_INTS {
      let divisor = U::from_u64(divisor);
      if gcd % divisor == U::ZERO && gcd != divisor {
        *counts_accounting_for_small_multiples
          .entry(gcd / divisor)
          .or_insert(0) += count;
      }
    }
  }

  let (candidate_gcd, _) = counts_accounting_for_small_multiples
    .iter()
    .filter_map(|(&gcd, &count)| {
      let score = score_triple_gcd(gcd, count, total_triples)?;
      Some((gcd, score))
    })
    .max_by_key(|(_, score)| score.to_unsigned())?;

  Some(candidate_gcd)
}

fn calc_candidate_base<U: UnsignedLike>(sample: &[U]) -> Option<U> {
  let triple_gcds = sample
    .chunks_exact(3)
    .map(calc_triple_gcd)
    .filter(|&gcd| gcd > U::ONE)
    .collect::<Vec<_>>();

  let candidate_gcd = most_prominent_gcd(&triple_gcds, sample.len() / 3)?;

  if !sampling::has_enough_infrequent_ints(sample, |x| x / candidate_gcd) {
    return None;
  }

  Some(candidate_gcd)
}

fn candidate_gcd_w_sample<U: UnsignedLike>(sample: &mut [U]) -> Option<U> {
  // should we switch sample to use a deterministic RNG so that we don't need to shuffle here?
  shuffle_sample(sample);
  calc_candidate_base(sample)
}

pub fn choose_base<T: NumberLike>(nums: &[T]) -> Option<T::Unsigned> {
  let mut sample = sampling::choose_sample(nums, |num| Some(num.to_unsigned()))?;
  let candidate = candidate_gcd_w_sample(&mut sample)?;
  // TODO validate adj distribution on entire `nums` is simple enough?
  Some(candidate)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_split_join_latents() {
    // SPLIT
    let nums = vec![-3, 1, 5];
    let latents = split_latents(&nums, 4_u32).per_var;
    assert_eq!(latents.len(), 2);
    assert_eq!(
      latents[0].latents,
      vec![536870911_u32, 536870912, 536870913]
    );
    assert_eq!(latents[1].latents, vec![1, 1, 1]);

    // JOIN
    let mut primary = latents[0].latents.clone();
    let mut secondary = latents[1].latents.clone();
    join_latents(
      4,
      &mut primary,
      SecondaryLatents::Nonconstant(&mut secondary),
    );

    assert_eq!(
      primary,
      nums.iter().map(|num| num.to_unsigned()).collect::<Vec<_>>()
    );
  }

  #[test]
  fn test_calc_gcd() {
    assert_eq!(calc_gcd(0_u32, 0), 0);
    assert_eq!(calc_gcd(0_u32, 1), 1);
    assert_eq!(calc_gcd(1_u32, 0), 1);
    assert_eq!(calc_gcd(2_u32, 0), 2);
    assert_eq!(calc_gcd(2_u32, 3), 1);
    assert_eq!(calc_gcd(6_u32, 3), 3);
    assert_eq!(calc_gcd(12_u32, 30), 6);
  }

  #[test]
  fn test_calc_triple_gcd() {
    assert_eq!(calc_triple_gcd(&[1_u32, 5, 9]), 4);
    assert_eq!(calc_triple_gcd(&[8_u32, 5, 2]), 3);
    assert_eq!(calc_triple_gcd(&[3_u32, 3, 3]), 0);
    assert_eq!(calc_triple_gcd(&[5_u32, 0, 10]), 5);
  }

  #[test]
  fn test_calc_candidate_gcd() {
    // not significant enough
    assert_eq!(
      calc_candidate_base(&mut vec![0_u32, 4, 8, 10, 14, 18]),
      None,
    );
    assert_eq!(
      calc_candidate_base(&mut vec![
        0_u32, 4, 8, 10, 14, 18, 20, 24, 28
      ]),
      Some(4),
    );
    // 2 out of 3 triples have a rare congruency
    assert_eq!(
      calc_candidate_base(&mut vec![
        1_u32, 11, 21, 31, 41, 51, 61, 71, 82
      ]),
      Some(10),
    );
    // 1 out of 3 triples has a rare congruency
    assert_eq!(
      calc_candidate_base(&mut vec![
        1_u32, 11, 22, 31, 41, 51, 61, 71, 82
      ]),
      None,
    );
  }
}
