use crate::bin::BinCompressionInfo;
use crate::constants::{Bitlen, Weight};
use crate::data_types::Latent;
use crate::{bits, sort_utils};
use std::cmp::{max, min};

// struct Bound<L: Latent> {
//   is_tight: bool,
//   value: L,
// }
//
struct Precomputed {
  n: usize,
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

fn calc_bin_idx(i: usize, precomputed: &Precomputed) -> usize {
  // 64-bit arithmetic here because otherwise it would go OOB on 32-bit arches
  (((i as u64) << precomputed.n_bins_log) / precomputed.n as u64) as usize
}

// the inverse of calc_bin_idx
fn calc_slice_idx(bin_idx: usize, precomputed: &Precomputed) -> usize {
  ((bin_idx as u64 * precomputed.n as u64) >> precomputed.n_bins_log) as usize
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
fn choose_pivot<L: Latent>(latents: &[L], lower: L, loose_upper: L) -> Option<L> {
  if lower == loose_upper {
    return None;
  }

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

  fn merge_incomplete(&mut self, latents: &[L], tight_lower: L) {
    if latents.is_empty() {
      return;
    }

    // TODO
    let upper = calc_max(latents);
    if let Some(bin) = self.incomplete_bin.as_mut() {
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

    let Some(pivot) = choose_pivot(latents, args.tight_lower, args.loose_upper) else {
      // everything is constant
      if args.max_bin_idx - args.min_bin_idx >= 2 {
        self.finish_incomplete_bin();
        self.merge_incomplete(latents, args.tight_lower);
        self.finish_incomplete_bin();
      } else {
        let target_count = calc_slice_idx(args.max_bin_idx, precomputed);
        if args.c_count + latents.len() - target_count > target_count - args.c_count {
          // better to emit what we have
          self.finish_incomplete_bin();
          self.merge_incomplete(latents, args.tight_lower);
        } else {
          // better to add this constant run first
          self.merge_incomplete(latents, args.tight_lower);
          self.finish_incomplete_bin();
        }
      }
      return;
    };
    let (lhs_count, _) = sort_utils::partition(latents, pivot);

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

  let precomputed = Precomputed {
    n: latents.len(),
    n_bins_log,
  };

  let mut state = State::new(n_bins_log);
  let tight_lower = calc_min(latents);
  state.exclusive_bins_quicksort_recurse(
    latents,
    &precomputed,
    RecurseArgs {
      c_count: 0,
      tight_lower,
      loose_upper: L::MAX,
      min_bin_idx: 0,
      max_bin_idx: 1 << n_bins_log,
    },
  );
  state.dst
}
