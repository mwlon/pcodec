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
  fn new(n: usize) -> Self {
    Self {
      lb: Bound::Loose(L::ZERO),
      ub: Bound::Loose(L::MAX),
      bad_pivot_limit: 1 + (n + 1).ilog2(),
    }
  }
}

#[derive(Debug, PartialEq, Eq)]
pub struct HistogramBin<L: Latent> {
  pub count: usize,
  pub lower: L,
  pub upper: L,
}

fn slice_min<L: Latent>(latents: &[L]) -> L {
  latents.iter().cloned().fold(L::MAX, min)
}

fn slice_max<L: Latent>(latents: &[L]) -> L {
  latents.iter().cloned().fold(L::ZERO, max)
}

fn slice_min_max<L: Latent>(latents: &[L]) -> (L, L) {
  latents.iter().cloned().fold(
    (L::MAX, L::ZERO),
    |(min_val, max_val), val| (min(min_val, val), max(max_val, val)),
  )
}

struct HistogramBuilder<L: Latent> {
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

impl<L: Latent> HistogramBuilder<L> {
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

    if let Some(bin) = self.incomplete_bin.as_mut() {
      bin.upper = match upper {
        Bound::Loose(_) => slice_max(latents),
        Bound::Tight(upper) => upper,
      };
      bin.count += latents.len();
    } else {
      let (tight_lb, tight_ub) = match (lower, upper) {
        (Bound::Loose(_), Bound::Loose(_)) => slice_min_max(latents),
        (Bound::Loose(_), Bound::Tight(upper)) => (slice_min(latents), upper),
        (Bound::Tight(lower), Bound::Loose(_)) => (lower, slice_max(latents)),
        (Bound::Tight(lower), Bound::Tight(upper)) => (lower, upper),
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
      self.dst.push(HistogramBin {
        count: bin.count,
        lower: bin.lower,
        upper: bin.upper,
      });
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

  #[inline(never)]
  fn apply_sorted(&mut self, mut latents: &[L]) {
    while !latents.is_empty() {
      let target_bin_idx = self.bin_idx(self.n_applied);
      debug_assert!(target_bin_idx >= self.next_avail_bin_idx);
      let target_c_count = self.c_count(target_bin_idx);
      let target_i = target_c_count - self.n_applied;

      if target_i >= latents.len() {
        self.apply_incomplete(
          latents,
          Bound::Tight(latents[0]),
          Bound::Tight(latents[latents.len() - 1]),
        );
        if target_i == latents.len() {
          self.complete_bin(target_bin_idx);
        }
        break;
      }

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
    }
  }

  fn apply_quicksort_recurse(&mut self, latents: &mut [L], args: RecurseArgs<L>) {
    if latents.is_empty() {
      return;
    }

    // TODO one day we should investigate whether there's a faster
    // selection algorithm for very short (len<20?) slices

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
    let mut bad_pivot_limit = args.bad_pivot_limit;
    let (lhs, rhs) = latents.split_at_mut(lhs_count);
    if was_bad_pivot {
      bad_pivot_limit -= 1;

      if bad_pivot_limit == 0 {
        sort_utils::heapsort(lhs);
        sort_utils::heapsort(rhs);
        self.apply_sorted(latents);
        return;
      }

      sort_utils::break_patterns(lhs);
      sort_utils::break_patterns(rhs);
    }

    self.apply_quicksort_recurse(
      lhs,
      RecurseArgs {
        lb: args.lb,
        ub: lhs_ub,
        bad_pivot_limit,
      },
    );
    self.apply_quicksort_recurse(
      rhs,
      RecurseArgs {
        lb: rhs_lb,
        ub: args.ub,
        bad_pivot_limit,
      },
    );
  }
}

// To compute unoptimized bins, we take a histogram of the data with the
// following properties:
// * there are up to 2^n_bins_log bins
// * each bin has an exclusive, tight range of data
// * we know the exact weight for each bin
//
// Previously we did a full sort and then something like the `apply_sorted`
// function above.
// However, the full sort is unnecessarily slow, especially when
// n_bins_log = 0, so we now only do a partial sort, avoiding sorting within
// each bin exactly.
pub fn histogram<L: Latent>(latents: &mut [L], n_bins_log: Bitlen) -> Vec<HistogramBin<L>> {
  let mut state = HistogramBuilder::new(latents.len(), n_bins_log);
  state.apply_quicksort_recurse(latents, RecurseArgs::new(latents.len()));
  state.dst
}

#[cfg(test)]
mod tests {
  use rand::seq::SliceRandom;
  use rand_xoshiro::rand_core::SeedableRng;
  use rand_xoshiro::Xoroshiro128PlusPlus;

  use super::*;

  fn make_bin(count: usize, lower: u32, upper: u32) -> HistogramBin<u32> {
    HistogramBin {
      count,
      lower,
      upper,
    }
  }

  fn run_sorted(
    latentss: &[Vec<u32>],
    n: usize,
    n_bins_log: Bitlen,
  ) -> (
    Vec<HistogramBin<u32>>,
    Option<HistogramBin<u32>>,
  ) {
    let mut state = HistogramBuilder::<u32>::new(n, n_bins_log);
    for latents in latentss {
      state.apply_sorted(latents);
    }
    (state.dst, state.incomplete_bin)
  }

  fn run_sorted_simple(latents: Vec<u32>, n_bins_log: Bitlen) -> Vec<HistogramBin<u32>> {
    let n = latents.len();
    let (bins, incomplete) = run_sorted(&[latents], n, n_bins_log);
    assert_eq!(incomplete, None);
    bins
  }

  fn run_quicksort(latents: &mut [u32], n_bins_log: Bitlen) -> Vec<HistogramBin<u32>> {
    let mut state = HistogramBuilder::<u32>::new(latents.len(), n_bins_log);
    let args = RecurseArgs::new(latents.len());
    state.apply_quicksort_recurse(latents, args);
    state.dst
  }

  #[test]
  fn test_bin_idx_and_c_count() {
    let state = HistogramBuilder::<u32>::new(41, 2);
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
  fn test_histogram_sorted_simple() {
    let latents = vec![];
    let bins = run_sorted_simple(latents, 2);
    assert_eq!(bins, vec![]);

    let latents = vec![8];
    let bins = run_sorted_simple(latents, 0);
    assert_eq!(bins, vec![make_bin(1, 8, 8)],);

    let latents = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
    let bins = run_sorted_simple(latents, 2);
    assert_eq!(
      bins,
      vec![
        make_bin(3, 1, 3),
        make_bin(2, 4, 5),
        make_bin(2, 6, 7),
        make_bin(2, 8, 9),
      ]
    );

    let latents = vec![8; 11];
    let bins = run_sorted_simple(latents, 2);
    assert_eq!(bins, vec![make_bin(11, 8, 8),]);

    let latents = vec![0, 0, 0, 1, 2, 2, 2, 2];
    let bins = run_sorted_simple(latents, 3);
    assert_eq!(
      bins,
      vec![make_bin(3, 0, 0), make_bin(1, 1, 1), make_bin(4, 2, 2),]
    );

    let latents = vec![0, 0, 1, 2, 2, 2, 2, 2];
    let bins = run_sorted_simple(latents, 3);
    assert_eq!(
      bins,
      vec![make_bin(2, 0, 0), make_bin(1, 1, 1), make_bin(5, 2, 2),]
    );
  }

  #[test]
  fn test_histogram_sorted_complex() {
    let latents = vec![vec![1, 2], vec![3, 4, 5], vec![6, 7], vec![8]];
    let (bins, incomplete) = run_sorted(&latents, 16, 3);
    assert_eq!(
      bins,
      vec![
        make_bin(2, 1, 2),
        make_bin(2, 3, 4),
        make_bin(2, 5, 6),
        make_bin(2, 7, 8),
      ]
    );
    assert_eq!(incomplete, None);

    let latents = vec![vec![1, 2, 3, 3, 3, 3, 3, 3, 3, 4], vec![5, 5, 5, 5]];
    let (bins, incomplete) = run_sorted(&latents, 16, 2);
    assert_eq!(
      bins,
      vec![make_bin(2, 1, 2), make_bin(7, 3, 3), make_bin(1, 4, 4)]
    );
    assert_eq!(incomplete, Some(make_bin(4, 5, 5)));

    let latents = vec![vec![1, 1, 2]];
    let (bins, incomplete) = run_sorted(&latents, 16, 2);
    assert_eq!(bins, vec![]);
    assert_eq!(incomplete, Some(make_bin(3, 1, 2)));
  }

  #[test]
  fn test_histogram_quicksort() {
    let mut latents = vec![];
    let bins = run_quicksort(&mut latents, 2);
    assert_eq!(bins, vec![]);

    let mut latents = vec![8];
    let bins = run_quicksort(&mut latents, 0);
    assert_eq!(bins, vec![make_bin(1, 8, 8)],);

    for seed in 0..16 {
      let mut rng = Xoroshiro128PlusPlus::seed_from_u64(seed);
      let mut latents = (0..100).collect::<Vec<_>>();
      latents.shuffle(&mut rng);

      let bins = run_quicksort(&mut latents, 2);
      assert_eq!(
        bins,
        vec![
          make_bin(25, 0, 24),
          make_bin(25, 25, 49),
          make_bin(25, 50, 74),
          make_bin(25, 75, 99),
        ]
      );

      let mut latents = vec![0; 100];
      latents[0] = 1;
      latents.shuffle(&mut rng);
      let bins = run_quicksort(&mut latents, 2);
      assert_eq!(
        bins,
        vec![make_bin(99, 0, 0), make_bin(1, 1, 1),]
      );

      let mut latents = vec![1; 100];
      latents[0] = 0;
      latents.shuffle(&mut rng);
      let bins = run_quicksort(&mut latents, 2);
      assert_eq!(
        bins,
        vec![make_bin(1, 0, 0), make_bin(99, 1, 1),]
      );

      let mut latents = [5; 100];
      latents[0] = 3;
      latents[1..3].fill(7);
      latents.shuffle(&mut rng);
      let bins = run_quicksort(&mut latents, 2);
      assert_eq!(
        bins,
        vec![make_bin(1, 3, 3), make_bin(97, 5, 5), make_bin(2, 7, 7),]
      );
      let bins = run_quicksort(&mut latents, 1);
      assert_eq!(
        bins,
        vec![make_bin(98, 3, 5), make_bin(2, 7, 7),]
      );

      let mut latents = [5; 100];
      latents[0..2].fill(3);
      latents[2] = 7;
      latents.shuffle(&mut rng);
      let bins = run_quicksort(&mut latents, 1);
      assert_eq!(
        bins,
        vec![make_bin(2, 3, 3), make_bin(98, 5, 7),]
      );
    }
  }
}
