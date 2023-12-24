use crate::bin::BinCompressionInfo;
use crate::bits;
use crate::constants::{Bitlen, Weight};
use crate::data_types::UnsignedLike;
use std::cmp::{max, min};

struct BinBuffer<'a, U: UnsignedLike> {
  pub seq: Vec<BinCompressionInfo<U>>,
  bin_idx: usize,
  max_n_bin: usize,
  n_unsigneds: usize,
  sorted: &'a [U],
  pub target_j: usize,
}

impl<'a, U: UnsignedLike> BinBuffer<'a, U> {
  fn calc_target_j(&mut self) {
    self.target_j = ((self.bin_idx + 1) * self.n_unsigneds) / self.max_n_bin
  }

  fn new(max_n_bin: usize, n_unsigneds: usize, sorted: &'a [U]) -> Self {
    let mut res = Self {
      seq: Vec::with_capacity(max_n_bin),
      bin_idx: 0,
      max_n_bin,
      n_unsigneds,
      sorted,
      target_j: 0,
    };
    res.calc_target_j();
    res
  }

  fn push_bin(&mut self, i: usize, j: usize) {
    let sorted = self.sorted;
    let n_unsigneds = self.n_unsigneds;

    let count = j - i;
    let new_bin_idx = max(
      self.bin_idx + 1,
      (j * self.max_n_bin) / n_unsigneds,
    );
    let lower = sorted[i];
    let upper = sorted[j - 1];

    let bin = BinCompressionInfo {
      weight: count as Weight,
      lower,
      upper,
      offset_bits: bits::bits_to_encode_offset(upper - lower),
      ..Default::default()
    };
    self.seq.push(bin);
    self.bin_idx = new_bin_idx;
    self.calc_target_j();
  }
}

#[inline(never)]
fn choose_unoptimized_bins_sorted<U: UnsignedLike>(
  sorted: &[U],
  unoptimized_bins_log: Bitlen,
) -> Vec<BinCompressionInfo<U>> {
  let n_unsigneds = sorted.len();
  let max_n_bins = min(1 << unoptimized_bins_log, n_unsigneds);

  let mut i = 0;
  let mut backup_j = 0_usize;
  let mut bin_buffer = BinBuffer::<U>::new(max_n_bins, n_unsigneds, sorted);

  for j in 1..n_unsigneds {
    let target_j = bin_buffer.target_j;
    if sorted[j] == sorted[j - 1] {
      if j >= target_j && j - target_j >= target_j - backup_j && backup_j > i {
        bin_buffer.push_bin(i, backup_j);
        i = backup_j;
      }
    } else {
      backup_j = j;
      if j >= target_j {
        bin_buffer.push_bin(i, j);
        i = j;
      }
    }
  }
  bin_buffer.push_bin(i, n_unsigneds);

  bin_buffer.seq
}

pub fn choose_unoptimized_bins<U: UnsignedLike>(
  deltas: Vec<U>,
  unoptimized_bins_log: Bitlen,
) -> Vec<BinCompressionInfo<U>> {
  let mut sorted = deltas;
  sorted.sort_unstable();
  choose_unoptimized_bins_sorted(&sorted, unoptimized_bins_log)
}
