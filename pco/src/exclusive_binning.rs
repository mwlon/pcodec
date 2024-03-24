use crate::bin::BinCompressionInfo;
use crate::bits;
use crate::constants::{Bitlen, Weight};
use crate::data_types::Latent;
use std::cmp::{max, min};
use std::mem;

const STRATEGY_NUMS_PER_BIN_THRESH: usize = 16;

#[derive(Clone, Copy)]
enum Strategy {
  MultiSelect,
  FullSort,
}

// struct Bound<L: Latent> {
//   is_tight: bool,
//   value: L,
// }
//
struct Precomputed {
  n: usize,
  n_bins_log: Bitlen,
  strategy: Strategy,
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

#[derive(Debug)]
struct RecurseArgs<L: Latent> {
  c_count: usize,
  tight_lower: L,
  loose_upper: L,
  min_bin_idx: usize,
  max_bin_idx: usize,
}

// impl<L: Latent> Bound<L> {
//   fn loose(value: L) -> Self {
//     Self {
//       is_tight: false,
//       value,
//     }
//   }
//
//   fn tight(value: L) -> Self {
//     Self {
//       is_tight: true,
//       value,
//     }
//   }
// }

fn calc_min_max<L: Latent>(latents: &[L]) -> (L, L) {
  let mut min_ = L::MAX;
  let mut max_ = L::ZERO;
  for &l in latents {
    // TODO make faster with SIMD?
    min_ = min(min_, l);
    max_ = max(max_, l);
  }
  (min_, max_)
}

fn calc_bin_idx(i: usize, precomputed: &Precomputed) -> usize {
  // 64-bit arithmetic here because otherwise it would go OOB on 32-bit arches
  (((i as u64) << precomputed.n_bins_log) / precomputed.n as u64) as usize
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

// median of first, middle, last
// idea from https://drops.dagstuhl.de/storage/00lipics/lipics-vol057-esa2016/LIPIcs.ESA.2016.38/LIPIcs.ESA.2016.38.pdf
// TODO if I keep this, write it better
// TODO try to make this more like highway and optionally target a percentile
fn choose_pivot<L: Latent>(latents: &[L], lower: L) -> Option<L> {
  let a = latents[0];
  let b = latents[latents.len() / 2];
  let c = latents[latents.len() - 1];

  let bc_max = max(b, c);
  let candidate = if a >= bc_max {
    bc_max
  } else {
    let bc_min = min(b, c);
    if a >= bc_min {
      a
    } else {
      bc_min
    }
  };

  if candidate == lower {
    for &l in latents {
      if l != lower {
        return Some(l);
      }
    }
    None
  } else {
    Some(candidate)
  }
}

fn do_pivot<L: Latent>(latents: &mut [L], pivot: L) -> usize {
  let mut i = 0;
  let mut j = latents.len();
  while i < j {
    // TODO make this fast
    if latents[i] < pivot {
      i += 1;
    } else {
      latents.swap(i, j - 1);
      j -= 1;
    }
  }
  i
}

impl<L: Latent> State<L> {
  fn new(n_bins_log: Bitlen) -> Self {
    Self {
      incomplete_bin: None,
      dst: Vec::with_capacity(1 << n_bins_log),
    }
  }

  fn merge_incomplete(&mut self, latents: &[L], tight_lower: L) {
    if latents.is_empty() {
      return;
    }

    // TODO
    let upper = latents.iter().cloned().max().unwrap();
    if let Some(mut bin) = self.incomplete_bin.as_mut() {
      bin.upper = upper;
      bin.count += latents.len();
    } else {
      self.incomplete_bin = Some(IncompleteBin {
        count: latents.len(),
        lower: tight_lower,
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
    if args.min_bin_idx == args.max_bin_idx {
      self.merge_incomplete(latents, args.tight_lower);
      return;
    }

    // TODO case when there are only a few latents

    let Some(pivot) = choose_pivot(latents, args.tight_lower) else {
      // everything is constant
      self.merge_incomplete(latents, args.tight_lower);
      self.finish_incomplete_bin();
      return;
    };
    let lhs_count = do_pivot(latents, pivot);

    let pivot_c_count = args.c_count + lhs_count;
    let pivot_bin_idx = calc_bin_idx(pivot_c_count, precomputed);

    self.exclusive_bins_quicksort_recurse(
      &mut latents[..lhs_count],
      precomputed,
      RecurseArgs {
        c_count: args.c_count,
        tight_lower: args.tight_lower,
        loose_upper: pivot,
        min_bin_idx: args.min_bin_idx,
        max_bin_idx: pivot_bin_idx,
      },
    );
    self.exclusive_bins_quicksort_recurse(
      &mut latents[lhs_count..],
      precomputed,
      RecurseArgs {
        c_count: args.c_count + lhs_count,
        tight_lower: pivot,
        loose_upper: args.loose_upper,
        min_bin_idx: pivot_bin_idx,
        max_bin_idx: args.max_bin_idx,
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

  let n_bins = 1_usize << n_bins_log;
  let strategy = if latents.len() >= STRATEGY_NUMS_PER_BIN_THRESH * n_bins {
    Strategy::MultiSelect
  } else {
    Strategy::FullSort
  };

  let precomputed = Precomputed {
    n: latents.len(),
    n_bins_log,
    strategy,
  };

  let mut state = State::new(n_bins_log);
  let (lower, upper) = calc_min_max(latents); // TODO
  state.exclusive_bins_quicksort_recurse(
    latents,
    &precomputed,
    RecurseArgs {
      c_count: 0,
      tight_lower: lower,
      loose_upper: upper,
      min_bin_idx: 0,
      max_bin_idx: n_bins,
    },
  );
  state.dst
}
