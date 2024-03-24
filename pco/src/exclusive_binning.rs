// TODO attribution for Rust unstable sort code
use crate::bin::BinCompressionInfo;
use crate::bits;
use crate::constants::{Bitlen, Weight};
use crate::data_types::Latent;
use std::cmp::{max, min};
use std::mem::MaybeUninit;
use std::{cmp, mem, ptr};

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

fn partition_in_blocks<L: Latent>(latents: &mut [L], pivot: L) -> usize {
  // Number of elements in a typical block.
  const BLOCK: usize = 128;

  // The partitioning algorithm repeats the following steps until completion:
  //
  // 1. Trace a block from the left side to identify elements greater than or equal to the pivot.
  // 2. Trace a block from the right side to identify elements smaller than the pivot.
  // 3. Exchange the identified elements between the left and right side.
  //
  // We keep the following variables for a block of elements:
  //
  // 1. `block` - Number of elements in the block.
  // 2. `start` - Start pointer into the `offsets` array.
  // 3. `end` - End pointer into the `offsets` array.
  // 4. `offsets` - Indices of out-of-order elements within the block.

  // The current block on the left side (from `l` to `l.add(block_l)`).
  let mut l = latents.as_mut_ptr();
  let mut block_l = BLOCK;
  let mut start_l = ptr::null_mut();
  let mut end_l = ptr::null_mut();
  let mut offsets_l = [MaybeUninit::<u8>::uninit(); BLOCK];

  // The current block on the right side (from `r.sub(block_r)` to `r`).
  // SAFETY: The documentation for .add() specifically mention that `vec.as_ptr().add(vec.len())` is always safe
  let mut r = unsafe { l.add(latents.len()) };
  let mut block_r = BLOCK;
  let mut start_r = ptr::null_mut();
  let mut end_r = ptr::null_mut();
  let mut offsets_r = [MaybeUninit::<u8>::uninit(); BLOCK];

  // FIXME: When we get VLAs, try creating one array of length `min(v.len(), 2 * BLOCK)` rather
  // than two fixed-size arrays of length `BLOCK`. VLAs might be more cache-efficient.

  // Returns the number of elements between pointers `l` (inclusive) and `r` (exclusive).
  fn width<T>(l: *mut T, r: *mut T) -> usize {
    assert!(mem::size_of::<T>() > 0);
    // FIXME: this should *likely* use `offset_from`, but more
    // investigation is needed (including running tests in miri).
    unsafe { r.offset_from(l) as usize }
    // (r.addr() - l.addr()) / mem::size_of::<T>()
  }

  loop {
    // We are done with partitioning block-by-block when `l` and `r` get very close. Then we do
    // some patch-up work in order to partition the remaining elements in between.
    let is_done = width(l, r) <= 2 * BLOCK;

    if is_done {
      // Number of remaining elements (still not compared to the pivot).
      let mut rem = width(l, r);
      if start_l < end_l || start_r < end_r {
        rem -= BLOCK;
      }

      // Adjust block sizes so that the left and right block don't overlap, but get perfectly
      // aligned to cover the whole remaining gap.
      if start_l < end_l {
        block_r = rem;
      } else if start_r < end_r {
        block_l = rem;
      } else {
        // There were the same number of elements to switch on both blocks during the last
        // iteration, so there are no remaining elements on either block. Cover the remaining
        // items with roughly equally-sized blocks.
        block_l = rem / 2;
        block_r = rem - block_l;
      }
      debug_assert!(block_l <= BLOCK && block_r <= BLOCK);
      debug_assert!(width(l, r) == block_l + block_r);
    }

    if start_l == end_l {
      // Trace `block_l` elements from the left side.
      start_l = offsets_l.as_mut_ptr().cast();
      // start_l = MaybeUninit::slice_as_mut_ptr(&mut offsets_l);
      end_l = start_l;
      let mut elem = l;

      for i in 0..block_l {
        // SAFETY: The unsafety operations below involve the usage of the `offset`.
        //         According to the conditions required by the function, we satisfy them because:
        //         1. `offsets_l` is stack-allocated, and thus considered separate allocated object.
        //         2. The function `is_less` returns a `bool`.
        //            Casting a `bool` will never overflow `isize`.
        //         3. We have guaranteed that `block_l` will be `<= BLOCK`.
        //            Plus, `end_l` was initially set to the begin pointer of `offsets_` which was declared on the stack.
        //            Thus, we know that even in the worst case (all invocations of `is_less` returns false) we will only be at most 1 byte pass the end.
        //        Another unsafety operation here is dereferencing `elem`.
        //        However, `elem` was initially the begin pointer to the slice which is always valid.
        unsafe {
          // Branchless comparison.
          *end_l = i as u8;
          end_l = end_l.add((*elem >= pivot) as usize);
          elem = elem.add(1);
        }
      }
    }

    if start_r == end_r {
      // Trace `block_r` elements from the right side.
      start_r = offsets_r.as_mut_ptr().cast();
      // start_r = MaybeUninit::slice_as_mut_ptr(&mut offsets_r);
      end_r = start_r;
      let mut elem = r;

      for i in 0..block_r {
        // SAFETY: The unsafety operations below involve the usage of the `offset`.
        //         According to the conditions required by the function, we satisfy them because:
        //         1. `offsets_r` is stack-allocated, and thus considered separate allocated object.
        //         2. The function `is_less` returns a `bool`.
        //            Casting a `bool` will never overflow `isize`.
        //         3. We have guaranteed that `block_r` will be `<= BLOCK`.
        //            Plus, `end_r` was initially set to the begin pointer of `offsets_` which was declared on the stack.
        //            Thus, we know that even in the worst case (all invocations of `is_less` returns true) we will only be at most 1 byte pass the end.
        //        Another unsafety operation here is dereferencing `elem`.
        //        However, `elem` was initially `1 * sizeof(T)` past the end and we decrement it by `1 * sizeof(T)` before accessing it.
        //        Plus, `block_r` was asserted to be less than `BLOCK` and `elem` will therefore at most be pointing to the beginning of the slice.
        unsafe {
          // Branchless comparison.
          elem = elem.sub(1);
          *end_r = i as u8;
          end_r = end_r.add((*elem < pivot) as usize);
        }
      }
    }

    // Number of out-of-order elements to swap between the left and right side.
    let count = cmp::min(width(start_l, end_l), width(start_r, end_r));

    if count > 0 {
      macro_rules! left {
        () => {
          l.add(usize::from(*start_l))
        };
      }
      macro_rules! right {
        () => {
          r.sub(usize::from(*start_r) + 1)
        };
      }

      // Instead of swapping one pair at the time, it is more efficient to perform a cyclic
      // permutation. This is not strictly equivalent to swapping, but produces a similar
      // result using fewer memory operations.

      // SAFETY: The use of `ptr::read` is valid because there is at least one element in
      // both `offsets_l` and `offsets_r`, so `left!` is a valid pointer to read from.
      //
      // The uses of `left!` involve calls to `offset` on `l`, which points to the
      // beginning of `v`. All the offsets pointed-to by `start_l` are at most `block_l`, so
      // these `offset` calls are safe as all reads are within the block. The same argument
      // applies for the uses of `right!`.
      //
      // The calls to `start_l.offset` are valid because there are at most `count-1` of them,
      // plus the final one at the end of the unsafe block, where `count` is the minimum number
      // of collected offsets in `offsets_l` and `offsets_r`, so there is no risk of there not
      // being enough elements. The same reasoning applies to the calls to `start_r.offset`.
      //
      // The calls to `copy_nonoverlapping` are safe because `left!` and `right!` are guaranteed
      // not to overlap, and are valid because of the reasoning above.
      unsafe {
        let tmp = ptr::read(left!());
        ptr::copy_nonoverlapping(right!(), left!(), 1);

        for _ in 1..count {
          start_l = start_l.add(1);
          ptr::copy_nonoverlapping(left!(), right!(), 1);
          start_r = start_r.add(1);
          ptr::copy_nonoverlapping(right!(), left!(), 1);
        }

        ptr::copy_nonoverlapping(&tmp, right!(), 1);
        mem::forget(tmp);
        start_l = start_l.add(1);
        start_r = start_r.add(1);
      }
    }

    if start_l == end_l {
      // All out-of-order elements in the left block were moved. Move to the next block.

      // block-width-guarantee
      // SAFETY: if `!is_done` then the slice width is guaranteed to be at least `2*BLOCK` wide. There
      // are at most `BLOCK` elements in `offsets_l` because of its size, so the `offset` operation is
      // safe. Otherwise, the debug assertions in the `is_done` case guarantee that
      // `width(l, r) == block_l + block_r`, namely, that the block sizes have been adjusted to account
      // for the smaller number of remaining elements.
      l = unsafe { l.add(block_l) };
    }

    if start_r == end_r {
      // All out-of-order elements in the right block were moved. Move to the previous block.

      // SAFETY: Same argument as [block-width-guarantee]. Either this is a full block `2*BLOCK`-wide,
      // or `block_r` has been adjusted for the last handful of elements.
      r = unsafe { r.sub(block_r) };
    }

    if is_done {
      break;
    }
  }

  // All that remains now is at most one block (either the left or the right) with out-of-order
  // elements that need to be moved. Such remaining elements can be simply shifted to the end
  // within their block.

  if start_l < end_l {
    // The left block remains.
    // Move its remaining out-of-order elements to the far right.
    debug_assert_eq!(width(l, r), block_l);
    while start_l < end_l {
      // remaining-elements-safety
      // SAFETY: while the loop condition holds there are still elements in `offsets_l`, so it
      // is safe to point `end_l` to the previous element.
      //
      // The `ptr::swap` is safe if both its arguments are valid for reads and writes:
      //  - Per the debug assert above, the distance between `l` and `r` is `block_l`
      //    elements, so there can be at most `block_l` remaining offsets between `start_l`
      //    and `end_l`. This means `r` will be moved at most `block_l` steps back, which
      //    makes the `r.offset` calls valid (at that point `l == r`).
      //  - `offsets_l` contains valid offsets into `v` collected during the partitioning of
      //    the last block, so the `l.offset` calls are valid.
      unsafe {
        end_l = end_l.sub(1);
        ptr::swap(l.add(usize::from(*end_l)), r.sub(1));
        r = r.sub(1);
      }
    }
    width(latents.as_mut_ptr(), r)
  } else if start_r < end_r {
    // The right block remains.
    // Move its remaining out-of-order elements to the far left.
    debug_assert_eq!(width(l, r), block_r);
    while start_r < end_r {
      // SAFETY: See the reasoning in [remaining-elements-safety].
      unsafe {
        end_r = end_r.sub(1);
        ptr::swap(l, r.sub(usize::from(*end_r) + 1));
        l = l.add(1);
      }
    }
    width(latents.as_mut_ptr(), l)
  } else {
    // Nothing else to do, we're done.
    width(latents.as_mut_ptr(), l)
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

pub(super) fn partition<L: Latent>(latents: &mut [L], pivot: L) -> (usize, bool) {
  let (mid, was_partitioned) = {
    // Place the pivot at the beginning of slice.
    // latents.swap(0, pivot);
    // let (pivot, v) = latents.split_at_mut(1);
    // let pivot = pivot[0];

    // Find the first pair of out-of-order elements.
    let mut l = 0;
    let mut r = latents.len();

    // SAFETY: The unsafety below involves indexing an array.
    // For the first one: We already do the bounds checking here with `l < r`.
    // For the second one: We initially have `l == 0` and `r == v.len()` and we checked that `l < r` at every indexing operation.
    //                     From here we know that `r` must be at least `r == l` which was shown to be valid from the first one.
    unsafe {
      // Find the first element greater than or equal to the pivot.
      while l < r && *latents.get_unchecked(l) < pivot {
        l += 1;
      }

      // Find the last element smaller that the pivot.
      while l < r && *latents.get_unchecked(r - 1) >= pivot {
        r -= 1;
      }
    }

    (
      l + partition_in_blocks(&mut latents[l..r], pivot),
      l >= r,
    )

    // `_pivot_guard` goes out of scope and writes the pivot (which is a stack-allocated
    // variable) back into the slice where it originally was. This step is critical in ensuring
    // safety!
  };

  // Place the pivot between the two partitions.
  // latents.swap(0, mid);

  (mid, was_partitioned)
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
    let (lhs_count, _) = partition(latents, pivot);

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
  let tight_lower = calc_min(latents); // TODO
  state.exclusive_bins_quicksort_recurse(
    latents,
    &precomputed,
    RecurseArgs {
      c_count: 0,
      tight_lower,
      loose_upper: L::MAX,
      min_bin_idx: 0,
      max_bin_idx: n_bins,
    },
  );
  state.dst
}
