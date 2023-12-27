use crate::bin::BinCompressionInfo;
use crate::bits;
use crate::constants::{Bitlen, Weight};
use crate::data_types::UnsignedLike;
use std::mem;

const BUCKETS_LOG: Bitlen = 8;
const N_BUCKETS: usize = 1 << BUCKETS_LOG;

// struct BinBuffer<'a, U: UnsignedLike> {
//   pub seq: Vec<BinCompressionInfo<U>>,
//   bin_idx: usize,
//   max_n_bin: usize,
//   n_unsigneds: usize,
//   sorted: &'a [U],
//   pub target_j: usize,
// }
//
// impl<'a, U: UnsignedLike> BinBuffer<'a, U> {
//   fn calc_target_j(&mut self) {
//     self.target_j = ((self.bin_idx + 1) * self.n_unsigneds) / self.max_n_bin
//   }
//
//   fn new(max_n_bin: usize, n_unsigneds: usize, sorted: &'a [U]) -> Self {
//     let mut res = Self {
//       seq: Vec::with_capacity(max_n_bin),
//       bin_idx: 0,
//       max_n_bin,
//       n_unsigneds,
//       sorted,
//       target_j: 0,
//     };
//     res.calc_target_j();
//     res
//   }
//
//   fn push_bin(&mut self, i: usize, j: usize) {
//     let sorted = self.sorted;
//     let n_unsigneds = self.n_unsigneds;
//
//     let count = j - i;
//     let new_bin_idx = max(
//       self.bin_idx + 1,
//       (j * self.max_n_bin) / n_unsigneds,
//     );
//     let lower = sorted[i];
//     let upper = sorted[j - 1];
//
//     let bin = BinCompressionInfo {
//       weight: count as Weight,
//       lower,
//       upper,
//       offset_bits: bits::bits_to_encode_offset(upper - lower),
//       ..Default::default()
//     };
//     self.seq.push(bin);
//     self.bin_idx = new_bin_idx;
//     self.calc_target_j();
//   }
// }
//
// #[inline(never)]
// fn choose_unoptimized_bins_sorted<U: UnsignedLike>(
//   sorted: &[U],
//   unoptimized_bins_log: Bitlen,
// ) -> Vec<BinCompressionInfo<U>> {
//   let n_unsigneds = sorted.len();
//   let max_n_bins = min(1 << unoptimized_bins_log, n_unsigneds);
//
//   let mut i = 0;
//   let mut backup_j = 0_usize;
//   let mut bin_buffer = BinBuffer::<U>::new(max_n_bins, n_unsigneds, sorted);
//
//   for j in 1..n_unsigneds {
//     let target_j = bin_buffer.target_j;
//     if sorted[j] == sorted[j - 1] {
//       if j >= target_j && j - target_j >= target_j - backup_j && backup_j > i {
//         bin_buffer.push_bin(i, backup_j);
//         i = backup_j;
//       }
//     } else {
//       backup_j = j;
//       if j >= target_j {
//         bin_buffer.push_bin(i, j);
//         i = j;
//       }
//     }
//   }
//   bin_buffer.push_bin(i, n_unsigneds);
//
//   bin_buffer.seq
// }

// pub fn choose_unoptimized_bins<U: UnsignedLike>(
//   deltas: Vec<U>,
//   unoptimized_bins_log: Bitlen,
// ) -> Vec<BinCompressionInfo<U>> {
//   let mut sorted = deltas;
//   sorted.sort_unstable();
//   choose_unoptimized_bins_sorted(&sorted, unoptimized_bins_log)
// }

#[derive(Clone, Copy, Default)]
struct IncompleteBin<U: UnsignedLike> {
  count: usize,
  lower: U,
  upper: U,
}

#[inline(never)]
fn merge_incomplete<U: UnsignedLike>(
  incomplete_bin: Option<IncompleteBin<U>>,
  deltas: &[U],
  bucket_lower: U,
) -> Option<IncompleteBin<U>> {
  if deltas.is_empty() {
    return incomplete_bin;
  }

  let upper = bucket_lower + deltas.iter().cloned().max().unwrap();
  if let Some(mut bin) = incomplete_bin {
    bin.upper = upper;
    bin.count += deltas.len();
    Some(bin)
  } else {
    if deltas.is_empty() {
      None
    } else {
      let lower = bucket_lower + deltas.iter().cloned().min().unwrap();
      Some(IncompleteBin {
        count: deltas.len(),
        lower,
        upper,
      })
    }
  }
}

struct UnoptimizedBinAfsState<U: UnsignedLike> {
  total_count: usize,
  target_n_bins: usize,
  dst: Vec<BinCompressionInfo<U>>,
}

fn make_info<U: UnsignedLike>(count: usize, lower: U, upper: U) -> BinCompressionInfo<U> {
  BinCompressionInfo {
    weight: count as Weight,
    lower,
    upper,
    offset_bits: bits::bits_to_encode_offset(upper - lower),
    ..Default::default()
  }
}

#[inline]
fn calc_bucket_idx<U: UnsignedLike>(delta: U, shift: Bitlen) -> usize {
  (delta >> shift).to_u64() as usize
}

#[inline(never)]
fn calc_bucket_counts<U: UnsignedLike>(deltas: &[U], shift: Bitlen) -> [usize; N_BUCKETS] {
  let mut bucket_counts = [0; N_BUCKETS];
  for &delta in deltas.iter() {
    bucket_counts[calc_bucket_idx(delta, shift)] += 1;
  }
  bucket_counts
}

fn radix_permute<U: UnsignedLike>(deltas: &mut [U], shift: Bitlen) -> [usize; N_BUCKETS + 1] {
  let bucket_counts = calc_bucket_counts(deltas, shift);

  let mut swap_idxs = [0; N_BUCKETS + 1];
  let mut s = 0;
  for (i, &count) in bucket_counts.iter().enumerate() {
    s += count;
    swap_idxs[i + 1] = s;
  }
  let start_idxs = swap_idxs.clone();

  // let mut bucket_idx = 0;
  // let mut bucket_end = start_idxs[1];
  // while bucket_idx < N_BUCKETS {
  //   let i = swap_idxs[bucket_idx];
  //   if i >= bucket_end {
  //     bucket_idx += 1;
  //     bucket_end = start_idxs.get(bucket_idx + 1).cloned().unwrap_or_default();
  //     continue;
  //   }
  //   let delta = deltas[i];
  //   let swap_bucket_idx = get_bucket_idx(delta);
  //   let swap_i = swap_idxs[swap_bucket_idx];
  //   deltas[i] = deltas[swap_i];
  //   deltas[swap_i] = delta;
  //   swap_idxs[swap_bucket_idx] += 1;
  // }

  for bucket_idx in 0..N_BUCKETS {
    let end = start_idxs[bucket_idx + 1];
    if swap_idxs[bucket_idx] == end {
      continue;
    }

    while swap_idxs[bucket_idx] < end {
      let i = swap_idxs[bucket_idx];
      let delta = deltas[i];
      let swap_bucket_idx = calc_bucket_idx(delta, shift);
      let swap_i = swap_idxs[swap_bucket_idx];
      deltas[i] = deltas[swap_i];
      deltas[swap_i] = delta;
      swap_idxs[swap_bucket_idx] += 1;
    }
  }

  // for sub_bucket_idx in 0..N_BUCKETS {
  //   for i in start_idxs[sub_bucket_idx]..start_idxs[sub_bucket_idx + 1] {
  //     assert_eq!(
  //       calc_bucket_idx(deltas[i], shift),
  //       sub_bucket_idx,
  //       "{} {}",
  //       i,
  //       deltas[i],
  //     );
  //   }
  // }

  start_idxs
}

impl<U: UnsignedLike> UnoptimizedBinAfsState<U> {
  fn push_info(&mut self, count: usize, lower: U, upper: U) {
    self.dst.push(make_info(count, lower, upper));
  }

  fn bin_end_counts(&self, c_count: usize, bucket_count: usize) -> Vec<usize> {
    let target_n_bins = self.target_n_bins;
    let first_bin_idx = (c_count * target_n_bins) / self.total_count;
    let last_bin_idx = ((c_count + bucket_count) * target_n_bins) / self.total_count;
    // only take the first 2 because we never use more than that
    (first_bin_idx..last_bin_idx)
      .take(2)
      .map(|bin_idx| ((bin_idx + 1) * self.total_count) / target_n_bins)
      .collect::<Vec<_>>()
  }

  fn afs(
    &mut self,
    c_count: usize,
    deltas: &mut [U],
    bucket_lower: U,
    depth: Bitlen,
    mut incomplete_bin: Option<IncompleteBin<U>>,
  ) -> Option<IncompleteBin<U>> {
    // 2 base cases and one recursion case
    let bucket_count = deltas.len();
    let bin_end_c_counts = self.bin_end_counts(c_count, bucket_count);

    // base case 1: There are no complete bins in this bucket. We update
    // incomplete bin information and return early.
    if bin_end_c_counts.is_empty() {
      return merge_incomplete(incomplete_bin, deltas, bucket_lower);
    }

    // base case 2: The bucket contains a single constant value. We either
    // combine this bucket with the incomplete bin information or push each one
    // separately.
    // let c_count_w_incomplete_bin = c_count + incomplete_bin.map(|bin| bin.count).unwrap_or_default();
    if depth == U::BITS / BUCKETS_LOG {
      let bucket_constant_value = bucket_lower;
      let first_target_c_count = bin_end_c_counts[0];
      return if c_count + bucket_count - first_target_c_count > first_target_c_count - c_count {
        // enough to warrant separate bins
        for bin in &incomplete_bin {
          self.push_info(bin.count, bin.lower, bin.upper);
        }

        if bin_end_c_counts.len() >= 2 {
          self.push_info(
            bucket_count,
            bucket_constant_value,
            bucket_constant_value,
          );
          None
        } else {
          Some(IncompleteBin {
            count: bucket_count,
            lower: bucket_constant_value,
            upper: bucket_constant_value,
          })
        }
      } else {
        // one bin
        let (incomplete_count, lower) = if let Some(bin) = incomplete_bin {
          (bin.count, bin.lower)
        } else {
          (0, bucket_constant_value)
        };
        self.push_info(
          incomplete_count + bucket_count,
          lower,
          bucket_constant_value,
        );
        None
      };
    }

    let height = U::BITS / BUCKETS_LOG - (depth + 1);
    let shift = height * BUCKETS_LOG;

    let start_idxs = radix_permute(deltas, shift);

    for i in 0..N_BUCKETS {
      let start = start_idxs[i];
      let end = start_idxs[i + 1];
      let d_bucket_lower = U::from_u64(i as u64) << shift;
      for delta in &mut deltas[start..end] {
        *delta -= d_bucket_lower;
      }
      incomplete_bin = self.afs(
        c_count + start,
        &mut deltas[start..end],
        bucket_lower + d_bucket_lower,
        depth + 1,
        incomplete_bin,
      );
    }

    incomplete_bin
  }
}

pub fn choose_unoptimized_bins<U: UnsignedLike>(
  deltas: &mut [U],
  unoptimized_bins_log: Bitlen,
) -> Vec<BinCompressionInfo<U>> {
  let mut state = UnoptimizedBinAfsState {
    total_count: deltas.len(),
    target_n_bins: 1 << unoptimized_bins_log,
    dst: Vec::new(),
  };
  state.afs(0, deltas, U::ZERO, 0, None);
  state.dst
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::ans::Token;

  #[test]
  fn test_make_info() {
    let dummy_token = Token::MAX;
    assert_eq!(
      make_info(0, 0_u32, 0),
      BinCompressionInfo {
        weight: 0,
        lower: 0,
        upper: 0,
        offset_bits: 0,
        token: dummy_token,
      }
    );
    assert_eq!(
      make_info(7, 13_u32, 25),
      BinCompressionInfo {
        weight: 7,
        lower: 13,
        upper: 25,
        offset_bits: 4,
        token: dummy_token,
      }
    );
  }

  #[test]
  fn test_radix_permute() {
    let mut deltas: Vec<u32> = vec![1, 2, 3, 0];
    let idxs = radix_permute(&mut deltas, 0);
    assert_eq!(deltas, vec![0, 1, 2, 3]);
    assert_eq!(&idxs[0..5], vec![0, 1, 2, 3, 4]);
  }

  #[test]
  fn test_bin_end_counts() {
    let state = UnoptimizedBinAfsState::<u32> {
      total_count: 14,
      target_n_bins: 4,
      dst: Vec::new(),
    };
    assert_eq!(state.bin_end_counts(0, 3), vec![]);
    assert_eq!(state.bin_end_counts(0, 4), vec![3]);
    assert_eq!(state.bin_end_counts(4, 2), vec![]);
    assert_eq!(state.bin_end_counts(4, 3), vec![7]);
    assert_eq!(state.bin_end_counts(4, 5), vec![7]);
    assert_eq!(state.bin_end_counts(4, 7), vec![7, 10]);
    assert_eq!(state.bin_end_counts(9, 5), vec![10, 14]);
  }

  #[test]
  fn test_choose_unoptimized_bins_initially_constant() {
    let deltas: Vec<u32> = vec![1, 1, 1, 1, 1, 1, 2];

    let bins_0 = choose_unoptimized_bins(&mut deltas.clone(), 0);
    let expected_0 = vec![make_info(7, 1, 2)];
    assert_eq!(bins_0, expected_0);

    for unoptimized_bins_log in [1, 2] {
      let bins = choose_unoptimized_bins(&mut deltas.clone(), unoptimized_bins_log);
      let expected = vec![make_info(6, 1, 1), make_info(1, 2, 2)];
      assert_eq!(bins, expected);
    }
  }

  #[test]
  fn test_choose_unoptimized_bins_finally_constant() {
    let deltas: Vec<u32> = vec![3, 3, 1, 3, 3, 3, 3];

    let bins_0 = choose_unoptimized_bins(&mut deltas.clone(), 0);
    let expected_0 = vec![make_info(7, 1, 3)];
    assert_eq!(bins_0, expected_0);

    for unoptimized_bins_log in [1, 2] {
      let bins = choose_unoptimized_bins(&mut deltas.clone(), unoptimized_bins_log);
      let expected = vec![make_info(1, 1, 1), make_info(6, 3, 3)];
      assert_eq!(bins, expected);
    }
  }

  #[test]
  fn test_choose_unoptimized_bins_incomplete() {
    let mut deltas: Vec<u32> = vec![0, 1, 2, 3, 4, 5, 6, 8, 8, 8, 10, 11, 12, 13, 14, 15];
    deltas.reverse();

    let bins = choose_unoptimized_bins(&mut deltas, 1);
    let expected = vec![make_info(7, 0, 6), make_info(9, 8, 15)];
    assert_eq!(bins, expected);
  }
}
