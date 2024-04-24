use std::cmp::min;
use std::collections::HashMap;
use std::f64::consts::PI;
use std::mem;

use crate::constants::MULT_REQUIRED_BITS_SAVED_PER_NUM;
use crate::data_types::{Latent, NumberLike};
use crate::sampling;

// riemann zeta function
const ZETA_OF_2: f64 = PI * PI / 6.0;
const LCB_RATIO: f64 = 1.0;

#[inline(never)]
pub fn split_latents<T: NumberLike>(nums: &[T], base: T::L) -> Vec<Vec<T::L>> {
  let n = nums.len();
  let mut mults = Vec::with_capacity(n);
  let mut adjs = Vec::with_capacity(n);
  unsafe {
    mults.set_len(n);
    adjs.set_len(n);
  }
  for (&num, (mult_dst, adj_dst)) in nums.iter().zip(mults.iter_mut().zip(adjs.iter_mut())) {
    let u = num.to_latent_ordered();
    // Maybe one day we could do a libdivide approach for these
    *mult_dst = u / base;
    *adj_dst = u % base;
  }
  vec![mults, adjs]
}

#[inline(never)]
pub(crate) fn join_latents<L: Latent>(base: L, primary: &mut [L], secondary: &[L]) {
  for (mult_and_dst, &adj) in primary.iter_mut().zip(secondary.iter()) {
    *mult_and_dst = (*mult_and_dst * base).wrapping_add(adj);
  }
}

fn calc_gcd<L: Latent>(mut x: L, mut y: L) -> L {
  if x == L::ZERO {
    return y;
  }

  loop {
    if y == L::ZERO {
      return x;
    }

    x %= y;
    mem::swap(&mut x, &mut y);
  }
}

fn biggest_cubic_root(a: f64, b: f64, c: f64, d: f64) -> Option<f64> {
  const MAX_STEPS: usize = 8;
  const X_TOLERANCE: f64 = 1E-4;
  // TODO comments
  let mut x = if a == 0.0 { 1.0 } else { (-d / a).cbrt() };
  let mut prev_x = x;
  for _ in 0..MAX_STEPS {
    let x2 = x * x;
    let x3 = x * x2;
    let val = a * x3 + b * x2 + c * x + d;
    let deriv = 3.0 * a * x2 + 2.0 * b * x + c;
    x -= val / deriv;

    if x < 0.0 || x > 1.0 {
      return None;
    }

    if (x - prev_x).abs() < X_TOLERANCE {
      return Some(x);
    }
    prev_x = x;
  }

  None
}

fn calc_triple_gcd<L: Latent>(triple: &[L]) -> L {
  let mut a = triple[0];
  let mut b = triple[1];
  let mut c = triple[2];
  // sort a, b, c
  if a > b {
    mem::swap(&mut a, &mut b);
  }
  if b > c {
    mem::swap(&mut b, &mut c);
  }
  if a > b {
    mem::swap(&mut a, &mut b);
  }

  calc_gcd(b - a, c - a)
}

fn filter_score_triple_gcd_float(
  gcd: f64,
  triples_w_gcd: usize,
  total_triples: usize,
) -> Option<f64> {
  if triples_w_gcd <= 1 {
    // not enough to make any claims
    return None;
  }

  let triples_w_gcd = triples_w_gcd as f64;
  let total_triples = total_triples as f64;
  let prob_per_triple = triples_w_gcd / total_triples;

  // check if the GCD has statistical evidence
  let natural_prob_per_triple = 1.0 / (ZETA_OF_2 * gcd * gcd);
  let stdev = (natural_prob_per_triple * (1.0 - natural_prob_per_triple) / total_triples).sqrt();
  let z_score = (prob_per_triple - natural_prob_per_triple) / stdev;
  if z_score < 3.0 {
    return None;
  }

  let triples_lcb = triples_w_gcd - LCB_RATIO * triples_w_gcd.sqrt();
  if triples_lcb <= 0.0 {
    return None;
  }
  let prob_per_triple_lcb = triples_lcb / total_triples;

  // heuristic for when the GCD is useless, even if true
  // We calculate the greatest possible entropy for the distribution, modulo
  // GCD (call it P(k)), assuming $\sum_k P(k)^2 = congruence_prob_per_pair$.
  // This occurs when there is one likely (concentrated) value of k, and the
  // rest are equally improbable.
  // You can use intuition or Lagrange multipliers to verify that.
  let congruence_prob_per_pair = (ZETA_OF_2 * prob_per_triple_lcb).min(1.0);
  let gcd_m1 = gcd - 1.0;
  let gcd_m1_inv_sq = 1.0 / (gcd_m1 * gcd_m1);
  let concentrated_p = biggest_cubic_root(
    1.0 - gcd_m1_inv_sq,
    3.0 * gcd_m1_inv_sq,
    -3.0 * gcd_m1_inv_sq,
    gcd_m1_inv_sq - congruence_prob_per_pair,
  )?;
  let worst_case_entropy_mod_gcd = -concentrated_p * concentrated_p.log2()
    - (1.0 - concentrated_p) * ((1.0 - concentrated_p) / gcd_m1).log2();
  let worst_case_bits_saved = gcd.log2() - worst_case_entropy_mod_gcd;
  // println!(
  //   "! %{} ({} / {}) {} {} {} {}",
  //   gcd,
  //   triples_lcb,
  //   total_triples,
  //   congruence_prob_per_pair,
  //   concentrated_p,
  //   worst_case_entropy_mod_gcd,
  //   worst_case_bits_saved
  // );
  if worst_case_bits_saved < MULT_REQUIRED_BITS_SAVED_PER_NUM {
    return None;
  }

  Some(worst_case_bits_saved)

  // The most likely valid GCD maximizes triples * gcd, and the most
  // valuable one (if true) maximizes triples.sqrt() * gcd. We take a
  // conservative lower confidence bound for how many triples we'd get if we
  // repeated the measurement, and strike a compromise between most likely and
  // most valuable.
  // if triples_lcb >= 0.0 {
  //   Some(triples_lcb.powf(0.6) * gcd)
  // } else {
  //   None
  // }
}

fn filter_score_triple_gcd<L: Latent>(
  gcd: L,
  triples_w_gcd: usize,
  total_triples: usize,
) -> Option<f64> {
  let gcd_f64 = min(gcd, L::from_u64(u64::MAX)).to_u64() as f64;
  filter_score_triple_gcd_float(gcd_f64, triples_w_gcd, total_triples)
}

fn most_prominent_gcd<L: Latent>(triple_gcds: &[L], total_triples: usize) -> Option<(L, f64)> {
  let mut counts = HashMap::new();
  for &gcd in triple_gcds {
    *counts.entry(gcd).or_insert(0) += 1;
  }

  let gcd_and_score = counts //counts_accounting_for_small_multiples
    .iter()
    .filter_map(|(&gcd, &count)| {
      let score = filter_score_triple_gcd(gcd, count, total_triples)?;
      Some((gcd, score))
    })
    .max_by_key(|(_, score)| score.to_latent_ordered())?;

  Some(gcd_and_score)
}

pub fn choose_candidate_base<L: Latent>(sample: &mut [L]) -> Option<(L, f64)> {
  let triple_gcds = sample
    .chunks_exact(3)
    .map(calc_triple_gcd)
    .filter(|&gcd| gcd > L::ONE)
    .collect::<Vec<_>>();

  most_prominent_gcd(&triple_gcds, sample.len() / 3)
}

pub fn choose_base<T: NumberLike>(nums: &[T]) -> Option<T::L> {
  let mut sample = sampling::choose_sample(nums, |num| Some(num.to_latent_ordered()))?;
  let (candidate, bits_saved_per_adj) = choose_candidate_base(&mut sample)?;

  if sampling::has_enough_infrequent_mults(
    &sample,
    |x| x / candidate,
    bits_saved_per_adj,
  ) {
    Some(candidate)
  } else {
    None
  }
}

#[cfg(test)]
mod tests {
  use rand::Rng;
  use rand_xoshiro::rand_core::SeedableRng;

  use super::*;

  #[test]
  fn test_split_join_latents() {
    // SPLIT
    let nums = vec![8_u32, 1, 5];
    let base = 4_u32;
    let latents = split_latents(&nums, base);
    assert_eq!(latents.len(), 2);
    assert_eq!(latents[0], vec![2_u32, 0, 1]);
    assert_eq!(latents[1], vec![0_u32, 1, 1]);

    // JOIN
    let mut primary_and_dst = latents[0].to_vec();
    join_latents(base, &mut primary_and_dst, &latents[1]);

    assert_eq!(primary_and_dst, nums);
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
      choose_candidate_base(&mut [0_u32, 4, 8]),
      None,
    );
    assert_eq!(
      choose_candidate_base(&mut [0_u32, 4, 8, 10, 14, 18, 20, 24, 28]),
      Some(4),
    );
    // 2 out of 3 triples have a rare congruency
    assert_eq!(
      choose_candidate_base(&mut [1_u32, 11, 21, 31, 41, 51, 61, 71, 82]),
      Some(10),
    );
    // 1 out of 3 triples has a rare congruency
    assert_eq!(
      choose_candidate_base(&mut [1_u32, 11, 22, 31, 41, 51, 61, 71, 82]),
      None,
    );
    // even just evens can be useful if the signal is strong enough
    let mut rng = rand_xoshiro::Xoroshiro128PlusPlus::seed_from_u64(0);
    let mut twos = (0_u32..200)
      .map(|_| rng.gen_range(0_u32..1000) * 2)
      .collect::<Vec<_>>();
    assert_eq!(choose_candidate_base(&mut twos), Some(2));
  }
}
