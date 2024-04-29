use std::cmp::{max, min};

use crate::bin::BinCompressionInfo;
use crate::constants::{Bitlen, Weight};
use crate::data_types::Latent;
use crate::sort_utils::heapsort;
use crate::{bits, sort_utils};

struct Precomputed {
  n: u64,
  n_bins_log: Bitlen,
}

#[derive(Debug)]
struct IncompleteBin<L: Latent> {
  count: usize,
  lower: L,
  upper: L,
}

struct State<L: Latent> {
  incomplete_bin: Option<IncompleteBin<L>>,
  dst: Vec<BinCompressionInfo<L>>,
}

#[derive(Clone, Copy, Debug)]
enum Bound<L: Latent> {
  Loose(L),
  Tight(L),
}

impl<L: Latent> Bound<L> {
  fn loose(&self) -> L {
    match self {
      Bound::Loose(x) => *x,
      Bound::Tight(x) => *x,
    }
  }
}

#[derive(Debug)]
struct RecurseArgs<L: Latent> {
  c_count: usize,
  lb: Bound<L>,
  ub: Bound<L>,
  min_bin_idx: usize,
  max_bin_idx: usize,
  bad_pivot_limit: u32,
}

impl<L: Latent> RecurseArgs<L> {
  fn new(n_bins_log: Bitlen) -> Self {
    Self {
      c_count: 0,
      lb: Bound::Loose(L::ZERO),
      ub: Bound::Loose(L::MAX),
      min_bin_idx: 0,
      max_bin_idx: 1 << n_bins_log,
      bad_pivot_limit: n_bins_log + 1,
    }
  }
}

fn calc_min<L: Latent>(latents: &[L]) -> L {
  let mut min0 = L::MAX;
  let mut min1 = L::MAX;
  for i in (0..latents.len()).skip(1).step_by(2) {
    min0 = min(min0, latents[i - 1]);
    min1 = min(min1, latents[i]);
  }
  if latents.len() % 2 == 1 {
    min0 = min(min0, latents.last().cloned().unwrap());
  }
  min(min0, min1)
}

fn calc_max<L: Latent>(latents: &[L]) -> L {
  let mut max0 = L::ZERO;
  let mut max1 = L::ZERO;
  for i in (0..latents.len()).skip(1).step_by(2) {
    max0 = max(max0, latents[i - 1]);
    max1 = max(max1, latents[i]);
  }
  if latents.len() % 2 == 1 {
    max0 = max(max0, latents.last().cloned().unwrap());
  }
  max(max0, max1)
}

fn bin_idx_from_c_count(i: usize, precomputed: &Precomputed) -> usize {
  // 64-bit arithmetic here because otherwise it would go OOB on 32-bit arches
  (((i as u64) << precomputed.n_bins_log) / precomputed.n) as usize
}

fn c_count_from_bin_idx(bin_idx: usize, precomputed: &Precomputed) -> usize {
  ((bin_idx as u64 * precomputed.n) >> precomputed.n_bins_log) as usize
}

fn make_info<L: Latent>(count: usize, lower: L, upper: L) -> BinCompressionInfo<L> {
  BinCompressionInfo {
    weight: count as Weight,
    lower,
    upper,
    offset_bits: bits::bits_to_encode_offset(upper - lower),
    ..Default::default()
  }
}

impl<L: Latent> State<L> {
  fn new(n_bins_log: Bitlen) -> Self {
    Self {
      incomplete_bin: None,
      dst: Vec::with_capacity(1 << n_bins_log),
    }
  }

  fn merge_incomplete(&mut self, latents: &[L], lower: Bound<L>, upper: Bound<L>) {
    if latents.is_empty() {
      return;
    }

    let tight_ub = match upper {
      Bound::Loose(_) => calc_max(latents),
      Bound::Tight(upper) => upper,
    };

    if let Some(bin) = self.incomplete_bin.as_mut() {
      bin.upper = tight_ub;
      bin.count += latents.len();
    } else {
      let tight_lb = match lower {
        Bound::Loose(_) => calc_min(latents),
        Bound::Tight(lower) => lower,
      };
      self.incomplete_bin = Some(IncompleteBin {
        count: latents.len(),
        lower: tight_lb,
        upper: tight_ub,
      });
    }
  }

  fn finish_incomplete_bin(&mut self) {
    if let Some(bin) = self.incomplete_bin.as_ref() {
      self.dst.push(make_info(bin.count, bin.lower, bin.upper));
      self.incomplete_bin = None;
    }
  }

  fn apply_constant_run(
    &mut self,
    latents: &[L],
    start: usize,
    target: usize,
    multiple_bins_available: bool,
  ) {
    let end = start + latents.len();
    let undershoot_better_than_overshoot = end - 1 - target > target - start;
    let const_bound = Bound::Tight(latents[0]);
    if multiple_bins_available {
      self.finish_incomplete_bin();
      self.merge_incomplete(latents, const_bound, const_bound);
      self.finish_incomplete_bin();
    } else if undershoot_better_than_overshoot {
      // better to emit what we have
      self.finish_incomplete_bin();
      self.merge_incomplete(latents, const_bound, const_bound);
    } else {
      // better to add this constant run first
      self.merge_incomplete(latents, const_bound, const_bound);
      self.finish_incomplete_bin();
    }
  }

  fn apply_latents_sorted(
    &mut self,
    mut latents: &[L],
    precomputed: &Precomputed,
    args: RecurseArgs<L>,
  ) {
    let mut next_bin_idx = args.min_bin_idx + 1;
    let mut c_count = args.c_count;
    let mut multiple_bins_available = false;

    while !latents.is_empty() {
      let target_c_count = c_count_from_bin_idx(next_bin_idx, &precomputed);
      let target_i = target_c_count - c_count - 1;
      let target_x = latents[target_i];

      let mut l = target_i;
      let mut r = target_i + 1;
      while l > 0 && latents[l - 1] == target_x {
        l -= 1;
      }
      while r < latents.len() && latents[r] == target_x {
        r += 1;
      }

      if l > 0 {
        self.merge_incomplete(
          &latents[..l],
          Bound::Tight(latents[0]),
          Bound::Tight(latents[l - 1]),
        );
      }

      self.apply_constant_run(
        &latents[l..r],
        l,
        target_i,
        multiple_bins_available,
      );

      latents = &latents[r..];
      c_count += r;
      let updated_bin_idx = max(
        next_bin_idx,
        bin_idx_from_c_count(c_count, &precomputed),
      ) + 1;
      multiple_bins_available = updated_bin_idx >= next_bin_idx + 2;
      next_bin_idx = updated_bin_idx;
    }

    self.finish_incomplete_bin();
  }

  fn apply_latents_quicksort_recurse(
    &mut self,
    latents: &mut [L],
    precomputed: &Precomputed,
    args: RecurseArgs<L>,
  ) {
    let next_target_c_count = c_count_from_bin_idx(args.min_bin_idx + 1, precomputed);
    let next_c_count = args.c_count + latents.len();
    if next_c_count <= next_target_c_count {
      self.merge_incomplete(latents, args.lb, args.ub);
      if next_c_count == next_target_c_count {
        self.finish_incomplete_bin();
      }
      return;
    }

    let loose_lb = args.lb.loose();
    if loose_lb == args.ub.loose() {
      // everything is constant
      let multiple_bins_available = args.max_bin_idx - args.min_bin_idx >= 2;
      self.apply_constant_run(
        latents,
        args.c_count,
        next_target_c_count,
        multiple_bins_available,
      );
      return;
    }

    let (tentative_pivot, _is_likely_sorted) = sort_utils::choose_pivot(latents);
    let (pivot, lhs_ub, rhs_lb) = if tentative_pivot > loose_lb {
      (
        tentative_pivot,
        Bound::Loose(tentative_pivot - L::ONE),
        Bound::Tight(tentative_pivot),
      )
    } else {
      (
        tentative_pivot + L::ONE,
        Bound::Tight(tentative_pivot),
        Bound::Loose(tentative_pivot + L::ONE),
      )
    };
    let (lhs_count, was_bad_pivot) = sort_utils::partition(latents, pivot);
    let bad_pivot_limit = args.bad_pivot_limit - (was_bad_pivot as u32);

    if bad_pivot_limit == 0 {
      sort_utils::heapsort(&mut latents[..lhs_count]);
      sort_utils::heapsort(&mut latents[lhs_count..]);
      self.apply_latents_sorted(latents, precomputed, args);
      return;
    }

    let pivot_c_count = args.c_count + lhs_count;
    let pivot_bin_idx = bin_idx_from_c_count(pivot_c_count, precomputed);

    self.apply_latents_quicksort_recurse(
      &mut latents[..lhs_count],
      precomputed,
      RecurseArgs {
        c_count: args.c_count,
        lb: args.lb,
        ub: lhs_ub,
        min_bin_idx: args.min_bin_idx,
        max_bin_idx: pivot_bin_idx,
        bad_pivot_limit,
      },
    );
    self.apply_latents_quicksort_recurse(
      &mut latents[lhs_count..],
      precomputed,
      RecurseArgs {
        c_count: args.c_count + lhs_count,
        lb: rhs_lb,
        ub: args.ub,
        min_bin_idx: pivot_bin_idx,
        max_bin_idx: args.max_bin_idx,
        bad_pivot_limit,
      },
    );
  }
}

pub fn exclusive_bins<L: Latent>(
  latents: &mut [L],
  n_bins_log: Bitlen,
) -> Vec<BinCompressionInfo<L>> {
  if latents.is_empty() {
    return vec![];
  }

  let precomputed = Precomputed {
    n: latents.len() as u64,
    n_bins_log,
  };

  let mut state = State::new(n_bins_log);
  // let tight_lower = calc_min(latents);
  // let mut is_sorted = true;
  // for (i, &x) in latents.iter().enumerate().skip(1) {
  //   if latents[i - 1] > x {
  //     is_sorted = false;
  //     break;
  //   }
  // }
  // if is_sorted {
  //   state.apply_latents_sorted(
  //     latents,
  //     &precomputed,
  //     RecurseArgs::new(n_bins_log),
  //   )
  // } else {
  state.apply_latents_quicksort_recurse(
    latents,
    &precomputed,
    RecurseArgs::new(n_bins_log),
  );
  // }
  state.dst
}

#[cfg(test)]
mod tests {
  use super::*;
  use rand::seq::SliceRandom;
  use rand::Rng;
  use rand_xoshiro::rand_core::{RngCore, SeedableRng};
  use rand_xoshiro::Xoroshiro128PlusPlus;

  fn run_exclusive_bins_sorted(
    latents: &[u32],
    n_bins_log: Bitlen,
  ) -> Vec<BinCompressionInfo<u32>> {
    let mut state = State::<u32>::new(n_bins_log);
    let precomputed = Precomputed {
      n: latents.len() as u64,
      n_bins_log,
    };
    let args = RecurseArgs::new(n_bins_log);
    state.apply_latents_sorted(latents, &precomputed, args);
    state.dst
  }

  fn run_exclusive_bins_quicksort(
    latents: &mut [u32],
    n_bins_log: Bitlen,
  ) -> Vec<BinCompressionInfo<u32>> {
    let mut state = State::<u32>::new(n_bins_log);
    let precomputed = Precomputed {
      n: latents.len() as u64,
      n_bins_log,
    };
    let args = RecurseArgs::new(n_bins_log);
    state.apply_latents_quicksort_recurse(latents, &precomputed, args);
    state.dst
  }

  #[test]
  fn test_exclusive_bins_sorted() {
    let latents = vec![];
    let bins = run_exclusive_bins_sorted(&latents, 2);
    assert_eq!(bins, vec![]);

    let latents = vec![8];
    let bins = run_exclusive_bins_sorted(&latents, 0);
    assert_eq!(bins, vec![make_info(1, 8_u32, 8)],);

    let latents = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
    let bins = run_exclusive_bins_sorted(&latents, 2);
    assert_eq!(
      bins,
      vec![
        make_info(2, 1_u32, 2),
        make_info(2, 3_u32, 4),
        make_info(2, 5_u32, 6),
        make_info(3, 7_u32, 9),
      ]
    );

    let latents = vec![8; 11];
    let bins = run_exclusive_bins_sorted(&latents, 2);
    assert_eq!(bins, vec![make_info(11, 8_u32, 8),]);

    let latents = vec![0, 0, 0, 1, 2, 2, 2, 2];
    let bins = run_exclusive_bins_sorted(&latents, 3);
    assert_eq!(
      bins,
      vec![
        make_info(3, 0_u32, 0),
        make_info(1, 1_u32, 1),
        make_info(4, 2_u32, 2),
      ]
    );

    let latents = vec![0, 0, 1, 2, 2, 2, 2, 2];
    let bins = run_exclusive_bins_sorted(&latents, 3);
    assert_eq!(
      bins,
      vec![
        make_info(2, 0_u32, 0),
        make_info(1, 1_u32, 1),
        make_info(5, 2_u32, 2),
      ]
    );
  }

  #[test]
  fn test_exclusive_bins_quicksort() {
    let mut latents = vec![];
    let bins = run_exclusive_bins_quicksort(&mut latents, 2);
    assert_eq!(bins, vec![]);

    let mut latents = vec![8];
    let bins = run_exclusive_bins_quicksort(&mut latents, 0);
    assert_eq!(bins, vec![make_info(1, 8_u32, 8)],);

    for seed in 0..16 {
      let mut rng = Xoroshiro128PlusPlus::seed_from_u64(seed);
      let mut latents = (0..100).collect::<Vec<_>>();
      latents.shuffle(&mut rng);

      let bins = run_exclusive_bins_quicksort(&mut latents, 2);
      assert_eq!(
        bins,
        vec![
          make_info(25, 0_u32, 24),
          make_info(25, 25_u32, 49),
          make_info(25, 50_u32, 74),
          make_info(25, 75_u32, 99),
        ]
      );

      let mut latents = vec![0; 100];
      latents[0] = 1;
      latents.shuffle(&mut rng);
      let bins = run_exclusive_bins_quicksort(&mut latents, 2);
      assert_eq!(
        bins,
        vec![make_info(99, 0_u32, 0), make_info(1, 1_u32, 1),]
      );

      let mut latents = [5; 100];
      latents[0] = 3;
      latents[1..3].fill(7);
      latents.shuffle(&mut rng);
      let bins = run_exclusive_bins_quicksort(&mut latents, 2);
      assert_eq!(
        bins,
        vec![
          make_info(1, 3_u32, 3),
          make_info(97, 5_u32, 5),
          make_info(2, 7_u32, 7),
        ]
      );
      let bins = run_exclusive_bins_quicksort(&mut latents, 1);
      assert_eq!(
        bins,
        vec![make_info(98, 3_u32, 5), make_info(2, 7_u32, 7),]
      );

      let mut latents = [5; 100];
      latents[0..2].fill(3);
      latents[2] = 7;
      latents.shuffle(&mut rng);
      let bins = run_exclusive_bins_quicksort(&mut latents, 1);
      assert_eq!(
        bins,
        vec![make_info(2, 3_u32, 3), make_info(98, 5_u32, 7),]
      );
    }
  }
}
