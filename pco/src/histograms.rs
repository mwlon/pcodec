use std::cmp::{max, min};

use crate::constants::Bitlen;
use crate::data_types::Latent;
use crate::sort_utils;

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
  lb: Bound<L>,
  ub: Bound<L>,
  bad_pivot_limit: u32,
}

impl<L: Latent> RecurseArgs<L> {
  fn new(n_bins_log: Bitlen) -> Self {
    Self {
      lb: Bound::Loose(L::ZERO),
      ub: Bound::Loose(L::MAX),
      bad_pivot_limit: n_bins_log + 1,
    }
  }
}

#[derive(Debug, PartialEq, Eq)]
pub struct HistogramBin<L: Latent> {
  pub count: usize,
  pub lower: L,
  pub upper: L,
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

fn make_bin<L: Latent>(count: usize, lower: L, upper: L) -> HistogramBin<L> {
  HistogramBin {
    count,
    lower,
    upper,
  }
}

struct State<L: Latent> {
  // immutable
  n: u64,
  n_bins: u64,
  n_bins_log: Bitlen,

  // mutable
  n_applied: usize,
  next_avail_bin_idx: usize,
  incomplete_bin: Option<HistogramBin<L>>,
  dst: Vec<HistogramBin<L>>,
}

impl<L: Latent> State<L> {
  fn new(n: usize, n_bins_log: Bitlen) -> Self {
    let n_bins = 1 << n_bins_log;
    Self {
      n: n as u64,
      n_bins,
      n_bins_log,
      n_applied: 0,
      next_avail_bin_idx: 0,
      incomplete_bin: None,
      dst: Vec::with_capacity(1 << n_bins_log),
    }
  }

  fn apply_incomplete(&mut self, latents: &[L], lower: Bound<L>, upper: Bound<L>) {
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
      self.incomplete_bin = Some(HistogramBin {
        count: latents.len(),
        lower: tight_lb,
        upper: tight_ub,
      });
    }
    self.n_applied += latents.len();
  }

  // true if anything was completed
  fn complete_bin(&mut self, bin_idx: usize) -> bool {
    if let Some(bin) = self.incomplete_bin.as_ref() {
      debug_assert!(bin_idx >= self.next_avail_bin_idx);
      self.next_avail_bin_idx = bin_idx + 1;
      self.dst.push(make_bin(bin.count, bin.lower, bin.upper));
      self.incomplete_bin = None;
      true
    } else {
      false
    }
  }

  fn bin_idx(&self, c_count: usize) -> usize {
    // 64-bit arithmetic here because otherwise it would go OOB on 32-bit arches
    (((c_count as u64) << self.n_bins_log) / self.n) as usize
  }

  fn c_count(&self, bin_idx: usize) -> usize {
    // ceiling of (bin_idx + 1) * n / n_bins
    (((bin_idx + 1) as u64 * self.n + self.n_bins - 1) >> self.n_bins_log) as usize
  }

  fn apply_constant_run(&mut self, latents: &[L]) {
    let start = self.n_applied;
    let mid = start + latents.len() / 2;
    let end = start + latents.len();
    let mut bin_idx = self.bin_idx(mid);
    if bin_idx > self.next_avail_bin_idx {
      // the previous bin idx is available, so we can either emit incomplete
      // stuff early or
      let spare_bin_idx = bin_idx - 1;
      if !self.complete_bin(spare_bin_idx) {
        bin_idx = spare_bin_idx;
      }
    }

    let const_bound = Bound::Tight(latents[0]);
    self.apply_incomplete(latents, const_bound, const_bound);
    if end >= self.c_count(bin_idx) {
      self.complete_bin(bin_idx);
    }
  }

  fn apply_sorted(&mut self, mut latents: &[L]) {
    let mut target_bin_idx = self.next_avail_bin_idx;

    while !latents.is_empty() {
      let target_c_count = self.c_count(target_bin_idx);
      let target_i = target_c_count - self.n_applied;

      let mut l = target_i - 1;
      let mut r = target_i;
      let target_x = latents[l];

      while l > 0 && latents[l - 1] == target_x {
        l -= 1;
      }
      while r < latents.len() && latents[r] == target_x {
        r += 1;
      }

      if l > 0 {
        self.apply_incomplete(
          &latents[..l],
          Bound::Tight(latents[0]),
          Bound::Tight(latents[l - 1]),
        );
      }

      self.apply_constant_run(&latents[l..r]);

      latents = &latents[r..];
      target_bin_idx = self.bin_idx(self.n_applied);
      debug_assert!(target_bin_idx >= self.next_avail_bin_idx);
    }
  }

  fn apply_quicksort_recurse(&mut self, latents: &mut [L], args: RecurseArgs<L>) {
    if latents.is_empty() {
      return;
    }

    let target_bin_idx = self.bin_idx(self.n_applied);
    let target_c_count = self.c_count(target_bin_idx);
    let end = self.n_applied + latents.len();
    if end <= target_c_count {
      self.apply_incomplete(latents, args.lb, args.ub);
      if end == target_c_count {
        self.complete_bin(target_bin_idx);
      }
      return;
    }

    let loose_lb = args.lb.loose();
    if loose_lb == args.ub.loose() || latents.len() == 1 {
      self.apply_constant_run(latents);
      return;
    }

    let tentative_pivot = sort_utils::choose_pivot(latents);
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
      self.apply_sorted(latents);
      return;
    }

    self.apply_quicksort_recurse(
      &mut latents[..lhs_count],
      RecurseArgs {
        lb: args.lb,
        ub: lhs_ub,
        bad_pivot_limit,
      },
    );
    self.apply_quicksort_recurse(
      &mut latents[lhs_count..],
      RecurseArgs {
        lb: rhs_lb,
        ub: args.ub,
        bad_pivot_limit,
      },
    );
  }
}

pub fn histogram<L: Latent>(latents: &mut [L], n_bins_log: Bitlen) -> Vec<HistogramBin<L>> {
  if latents.is_empty() {
    return vec![];
  }

  let mut state = State::new(latents.len(), n_bins_log);
  state.apply_quicksort_recurse(latents, RecurseArgs::new(n_bins_log));
  state.dst
}

#[cfg(test)]
mod tests {
  use rand::seq::SliceRandom;
  use rand_xoshiro::rand_core::SeedableRng;
  use rand_xoshiro::Xoroshiro128PlusPlus;

  use super::*;

  fn run_sorted(latents: &[u32], n_bins_log: Bitlen) -> Vec<HistogramBin<u32>> {
    let mut state = State::<u32>::new(latents.len(), n_bins_log);
    state.apply_sorted(latents);
    state.dst
  }

  fn run_quicksort(latents: &mut [u32], n_bins_log: Bitlen) -> Vec<HistogramBin<u32>> {
    let mut state = State::<u32>::new(latents.len(), n_bins_log);
    let args = RecurseArgs::new(n_bins_log);
    state.apply_quicksort_recurse(latents, args);
    state.dst
  }

  #[test]
  fn test_bin_idx_and_c_count() {
    let state = State::<u32>::new(41, 2);
    assert_eq!(state.bin_idx(0), 0);
    assert_eq!(state.bin_idx(10), 0);
    assert_eq!(state.bin_idx(11), 1);
    assert_eq!(state.c_count(0), 11);

    assert_eq!(state.bin_idx(30), 2);
    assert_eq!(state.bin_idx(31), 3);
    assert_eq!(state.bin_idx(40), 3);
    assert_eq!(state.c_count(3), 41);
  }

  #[test]
  fn test_histogram_sorted() {
    let latents = vec![];
    let bins = run_sorted(&latents, 2);
    assert_eq!(bins, vec![]);

    let latents = vec![8];
    let bins = run_sorted(&latents, 0);
    assert_eq!(bins, vec![make_bin(1, 8_u32, 8)],);

    let latents = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
    let bins = run_sorted(&latents, 2);
    assert_eq!(
      bins,
      vec![
        make_bin(3, 1_u32, 3),
        make_bin(2, 4_u32, 5),
        make_bin(2, 6_u32, 7),
        make_bin(2, 8_u32, 9),
      ]
    );

    let latents = vec![8; 11];
    let bins = run_sorted(&latents, 2);
    assert_eq!(bins, vec![make_bin(11, 8_u32, 8),]);

    let latents = vec![0, 0, 0, 1, 2, 2, 2, 2];
    let bins = run_sorted(&latents, 3);
    assert_eq!(
      bins,
      vec![
        make_bin(3, 0_u32, 0),
        make_bin(1, 1_u32, 1),
        make_bin(4, 2_u32, 2),
      ]
    );

    let latents = vec![0, 0, 1, 2, 2, 2, 2, 2];
    let bins = run_sorted(&latents, 3);
    assert_eq!(
      bins,
      vec![
        make_bin(2, 0_u32, 0),
        make_bin(1, 1_u32, 1),
        make_bin(5, 2_u32, 2),
      ]
    );
  }

  #[test]
  fn test_histogram_quicksort() {
    let mut latents = vec![];
    let bins = run_quicksort(&mut latents, 2);
    assert_eq!(bins, vec![]);

    let mut latents = vec![8];
    let bins = run_quicksort(&mut latents, 0);
    assert_eq!(bins, vec![make_bin(1, 8_u32, 8)],);

    for seed in 0..16 {
      let mut rng = Xoroshiro128PlusPlus::seed_from_u64(seed);
      let mut latents = (0..100).collect::<Vec<_>>();
      latents.shuffle(&mut rng);

      let bins = run_quicksort(&mut latents, 2);
      assert_eq!(
        bins,
        vec![
          make_bin(25, 0_u32, 24),
          make_bin(25, 25_u32, 49),
          make_bin(25, 50_u32, 74),
          make_bin(25, 75_u32, 99),
        ]
      );

      let mut latents = vec![0; 100];
      latents[0] = 1;
      latents.shuffle(&mut rng);
      let bins = run_quicksort(&mut latents, 2);
      assert_eq!(
        bins,
        vec![make_bin(99, 0_u32, 0), make_bin(1, 1_u32, 1),]
      );

      let mut latents = vec![1; 100];
      latents[0] = 0;
      latents.shuffle(&mut rng);
      let bins = run_quicksort(&mut latents, 2);
      assert_eq!(
        bins,
        vec![make_bin(1, 0_u32, 0), make_bin(99, 1_u32, 1),]
      );

      let mut latents = [5; 100];
      latents[0] = 3;
      latents[1..3].fill(7);
      latents.shuffle(&mut rng);
      let bins = run_quicksort(&mut latents, 2);
      assert_eq!(
        bins,
        vec![
          make_bin(1, 3_u32, 3),
          make_bin(97, 5_u32, 5),
          make_bin(2, 7_u32, 7),
        ]
      );
      let bins = run_quicksort(&mut latents, 1);
      assert_eq!(
        bins,
        vec![make_bin(98, 3_u32, 5), make_bin(2, 7_u32, 7),]
      );

      let mut latents = [5; 100];
      latents[0..2].fill(3);
      latents[2] = 7;
      latents.shuffle(&mut rng);
      let bins = run_quicksort(&mut latents, 1);
      assert_eq!(
        bins,
        vec![make_bin(2, 3_u32, 3), make_bin(98, 5_u32, 7),]
      );
    }
  }
}
