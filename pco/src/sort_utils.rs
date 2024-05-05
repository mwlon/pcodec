// The bulk of this file was originally copied from Rust's sort.rs,
// Licensed under Apache 2, i.e.
// https://github.com/rust-lang/rust/blob/02f7806ecd641d67c8f046b073323c7e176ee6d2/library/core/src/slice/sort.rs
// Modifications have been applied.
// Original Rust notice regarding their use of PDQ sort:
//   This module contains a sorting algorithm based on Orson Peters' pattern-defeating quicksort,
//   published at: <https://github.com/orlp/pdqsort>
//
// It was necessary to copy much of the code, since the sort_unstable API
// (understandably) does not expose the pivot function and such, but we want to
// drive pivoting from a faster histogram algorithm rather than completing a
// full sort.
use std::mem::MaybeUninit;
use std::{cmp, ptr};

use crate::data_types::Latent;

pub fn choose_pivot<L: Latent>(latents: &mut [L]) -> L {
  // Minimum length to choose the median-of-medians method.
  // Shorter slices use the simple median-of-three method.
  const SHORTEST_MEDIAN_OF_MEDIANS: usize = 50;

  let len = latents.len();

  // Three indices near which we are going to choose a pivot.
  let mut a = len / 4;
  let mut b = len / 2;
  let mut c = (len * 3) / 4;

  if len >= 8 {
    // Swaps indices so that `v[a] <= v[b]`.
    // SAFETY: `len >= 8` so there are at least two elements in the neighborhoods of
    // `a`, `b` and `c`. This means the three calls to `sort_adjacent` result in
    // corresponding calls to `sort3` with valid 3-item neighborhoods around each
    // pointer, which in turn means the calls to `sort2` are done with valid
    // references. Thus the `v.get_unchecked` calls are safe, as is the `ptr::swap`
    // call.
    let sort2 = |a: &mut usize, b: &mut usize| unsafe {
      if *latents.get_unchecked(*b) < *latents.get_unchecked(*a) {
        ptr::swap(a, b);
      }
    };

    // Swaps indices so that `v[a] <= v[b] <= v[c]`.
    let sort3 = |a: &mut usize, b: &mut usize, c: &mut usize| {
      sort2(a, b);
      sort2(b, c);
      sort2(a, b);
    };

    if len >= SHORTEST_MEDIAN_OF_MEDIANS {
      // Finds the median of `v[a - 1], v[a], v[a + 1]` and stores the index into `a`.
      let sort_adjacent = |a: &mut usize| {
        let tmp = *a;
        sort3(&mut (tmp - 1), a, &mut (tmp + 1));
      };

      // Find medians in the neighborhoods of `a`, `b`, and `c`.
      sort_adjacent(&mut a);
      sort_adjacent(&mut b);
      sort_adjacent(&mut c);
    }

    // Find the median among `a`, `b`, and `c`.
    sort3(&mut a, &mut b, &mut c);
  }

  latents[b]
}

// Scatters some elements around in an attempt to break patterns that might cause imbalanced
// partitions in quicksort.
#[cold]
pub fn break_patterns<L>(v: &mut [L]) {
  let len = v.len();
  if len >= 8 {
    let mut seed = len;
    let mut gen_usize = || {
      // Pseudorandom number generator from the "Xorshift RNGs" paper by George Marsaglia.
      if usize::BITS <= 32 {
        let mut r = seed as u32;
        r ^= r << 13;
        r ^= r >> 17;
        r ^= r << 5;
        seed = r as usize;
        seed
      } else {
        let mut r = seed as u64;
        r ^= r << 13;
        r ^= r >> 7;
        r ^= r << 17;
        seed = r as usize;
        seed
      }
    };

    // Take random numbers modulo this number.
    // The number fits into `usize` because `len` is not greater than `isize::MAX`.
    let modulus = len.next_power_of_two();

    // Some pivot candidates will be in the nearby of this index. Let's randomize them.
    let pos = len / 4 * 2;

    for i in 0..3 {
      // Generate a random number modulo `len`. However, in order to avoid costly operations
      // we first take it modulo a power of two, and then decrease by `len` until it fits
      // into the range `[0, len - 1]`.
      let mut other = gen_usize() & (modulus - 1);

      // `other` is guaranteed to be less than `2 * len`.
      if other >= len {
        other -= len;
      }

      v.swap(pos - 1 + i, other);
    }
  }
}

fn partition_in_blocks<L: Latent>(latents: &mut [L], pivot: L) -> usize {
  // Number of elements in a typical block.
  // This cannot exceed 256 due to how we store certain offsets in u8's
  const BLOCK_N: usize = 256;

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
  let mut block_l = BLOCK_N;
  let mut start_l = ptr::null_mut();
  let mut end_l = ptr::null_mut();
  let mut offsets_l = [MaybeUninit::<u8>::uninit(); BLOCK_N];

  // The current block on the right side (from `r.sub(block_r)` to `r`).
  // SAFETY: The documentation for .add() specifically mention that `vec.as_ptr().add(vec.len())` is always safe
  let mut r = unsafe { l.add(latents.len()) };
  let mut block_r = BLOCK_N;
  let mut start_r = ptr::null_mut();
  let mut end_r = ptr::null_mut();
  let mut offsets_r = [MaybeUninit::<u8>::uninit(); BLOCK_N];

  // Returns the number of elements between pointers `l` (inclusive) and `r` (exclusive).
  fn width<T>(l: *mut T, r: *mut T) -> usize {
    unsafe { r.offset_from(l) as usize }
  }

  loop {
    // We are done with partitioning block-by-block when `l` and `r` get very close. Then we do
    // some patch-up work in order to partition the remaining elements in between.
    let is_done = width(l, r) <= 2 * BLOCK_N;

    if is_done {
      // Number of remaining elements (still not compared to the pivot).
      let mut rem = width(l, r);
      if start_l < end_l || start_r < end_r {
        rem -= BLOCK_N;
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
      debug_assert!(block_l <= BLOCK_N && block_r <= BLOCK_N);
      debug_assert!(width(l, r) == block_l + block_r);
    }

    if start_l == end_l {
      // Trace `block_l` elements from the left side.
      start_l = offsets_l.as_mut_ptr().cast();
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
          // This is where things break if BLOCK_SIZE > 256
          *end_l = i as u8; // could be as much as BLOCK_SIZE - 1
          end_l = end_l.add((*elem >= pivot) as usize);
          elem = elem.add(1);
        }
      }
    }

    if start_r == end_r {
      // Trace `block_r` elements from the right side.
      start_r = offsets_r.as_mut_ptr().cast();
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

// returns (count on left side of pivot, was_bad_pivot)
pub fn partition<L: Latent>(latents: &mut [L], pivot: L) -> (usize, bool) {
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

  // We could also return whether the data was already partitioned as `l >= r`
  // like rust unstable sort does, but I'm not implementing that for now.
  let l_count = l + partition_in_blocks(&mut latents[l..r], pivot);
  let was_bad_pivot = cmp::min(l_count, latents.len() - l_count) < latents.len() / 8;
  (l_count, was_bad_pivot)
}

// Sorts `v` using heapsort, which guarantees *O*(*n* \* log(*n*)) worst-case.
#[cold]
pub fn heapsort<L: Latent>(latents: &mut [L]) {
  // This binary heap respects the invariant `parent >= child`.
  let sift_down = |x: &mut [L], mut node| {
    loop {
      // Children of `node`.
      let mut child = 2 * node + 1;
      if child >= x.len() {
        break;
      }

      // Choose the greater child.
      if child + 1 < x.len() {
        // We need a branch to be sure not to out-of-bounds index,
        // but it's highly predictable.  The comparison, however,
        // is better done branchless, especially for primitives.
        child += (x[child] < x[child + 1]) as usize;
      }

      // Stop if the invariant holds at `node`.
      if x[node] >= x[child] {
        break;
      }

      // Swap `node` with the greater child, move one step down, and continue sifting.
      x.swap(node, child);
      node = child;
    }
  };

  // Build the heap in linear time.
  for i in (0..latents.len() / 2).rev() {
    sift_down(latents, i);
  }

  // Pop maximal elements from the heap.
  for i in (1..latents.len()).rev() {
    latents.swap(0, i);
    sift_down(&mut latents[..i], 0);
  }
}
