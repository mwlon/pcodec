use crate::bin::BinCompressionInfo;
use crate::constants::{Bitlen, Weight};
use crate::data_types::Latent;
use crate::{bits, sort_utils};
use std::cmp::{max, min};
use std::mem;

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
  fn value(&self) -> L {
    match self {
      Bound::Loose(x) => *x,
      Bound::Tight(x) => *x,
    }
  }
}

#[derive(Debug)]
struct RecurseArgs<L: Latent> {
  c_count: usize,
  lower: Bound<L>,
  loose_upper: L,
  min_bin_idx: usize,
  max_bin_idx: usize,
  last_pivot: Option<L>,
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

// #[inline(never)]
// fn partition<L: Latent>(latents: &mut [L], pivot: L) -> usize {
//   let mut i = 0;
//   let mut j = latents.len();
//   while i < j {
//     // TODO make this fast
//     if latents[i] < pivot {
//       i += 1;
//       std::slice:
//     } else {
//       latents.swap(i, j - 1);
//       j -= 1;
//     }
//     // let is_lt_pivot = latents[i] < pivot;
//     // let lhs_incr = is_lt_pivot as usize;
//     // let rhs_decr = 1 - is_lt_pivot as usize;
//     // let swap_idx = if is_lt_pivot { i } else { j - 1 };
//     // latents.swap(i, swap_idx);
//     // i += lhs_incr;
//     // j -= rhs_decr;
//   }
//   i
// }

impl<L: Latent> State<L> {
  fn new(n_bins_log: Bitlen) -> Self {
    Self {
      incomplete_bin: None,
      dst: Vec::with_capacity(1 << n_bins_log),
    }
  }

  fn merge_incomplete(&mut self, latents: &[L], lower: &mut Bound<L>) {
    if latents.is_empty() {
      return;
    }

    // TODO
    let upper = calc_max(latents);
    if let Some(bin) = self.incomplete_bin.as_mut() {
      bin.upper = upper;
      bin.count += latents.len();
    } else {
      let latents_min = calc_min(latents);
      *lower = Bound::Tight(latents_min);
      self.incomplete_bin = Some(IncompleteBin {
        count: latents.len(),
        lower: latents_min,
        upper,
      });
    }
  }

  fn finish_incomplete_bin(&mut self) {
    if let Some(bin) = self.incomplete_bin.as_ref() {
      self.dst.push(make_info(bin.count, bin.lower, bin.upper));
      self.incomplete_bin = None;
    }
  }

  fn exclusive_bins_quicksort_recurse(
    &mut self,
    latents: &mut [L],
    precomputed: &Precomputed,
    args: RecurseArgs<L>,
  ) {
    let next_target_c_count = c_count_from_bin_idx(args.min_bin_idx + 1, precomputed);
    let next_c_count = args.c_count + latents.len();
    let mut lower = args.lower;
    if next_c_count <= next_target_c_count {
      self.merge_incomplete(latents, &mut lower);
      if next_c_count == next_target_c_count {
        self.finish_incomplete_bin();
      }
      return;
    }

    // TODO case when there are only a few latents

    if lower.value() == args.loose_upper {
      // everything is constant
      let undershoot_better_than_overshoot =
        next_c_count - next_target_c_count > next_target_c_count - args.c_count;
      if args.max_bin_idx - args.min_bin_idx >= 2 {
        self.finish_incomplete_bin();
        self.merge_incomplete(latents, &mut lower);
        self.finish_incomplete_bin();
      } else if undershoot_better_than_overshoot {
        // better to emit what we have
        self.finish_incomplete_bin();
        self.merge_incomplete(latents, &mut lower);
      } else {
        // better to add this constant run first
        self.merge_incomplete(latents, &mut lower);
        self.finish_incomplete_bin();
      }
      return;
    }

    let (mut pivot, is_likely_sorted) = sort_utils::choose_pivot(latents);
    let rhs_lower = if pivot == lower.value() {
      // TODO fix this hack
      pivot = pivot + L::ONE;
      Bound::Loose(pivot)
    } else {
      Bound::Tight(pivot)
    };
    // println!(
    //   "lb {:?} < pivot {} <= {} vs {:?} ({})",
    //   lower,
    //   pivot,
    //   args.loose_upper,
    //   args.last_pivot,
    //   latents.len(),
    // );
    let (lhs_count, _) = sort_utils::partition(latents, pivot);

    let pivot_c_count = args.c_count + lhs_count;
    let pivot_bin_idx = bin_idx_from_c_count(pivot_c_count, precomputed);

    self.exclusive_bins_quicksort_recurse(
      &mut latents[..lhs_count],
      precomputed,
      RecurseArgs {
        c_count: args.c_count,
        lower,
        loose_upper: pivot - L::ONE,
        min_bin_idx: args.min_bin_idx,
        max_bin_idx: pivot_bin_idx,
        last_pivot: None,
      },
    );
    self.exclusive_bins_quicksort_recurse(
      &mut latents[lhs_count..],
      precomputed,
      RecurseArgs {
        c_count: args.c_count + lhs_count,
        lower: rhs_lower,
        loose_upper: args.loose_upper,
        min_bin_idx: pivot_bin_idx,
        max_bin_idx: args.max_bin_idx,
        last_pivot: Some(pivot),
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
  state.exclusive_bins_quicksort_recurse(
    latents,
    &precomputed,
    RecurseArgs {
      c_count: 0,
      lower: Bound::Loose(L::ZERO),
      loose_upper: L::MAX,
      min_bin_idx: 0,
      max_bin_idx: 1 << n_bins_log,
      last_pivot: None,
    },
  );
  state.dst
}
