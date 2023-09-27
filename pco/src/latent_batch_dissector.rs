use crate::ans;
use crate::ans::{AnsState, Token};
use crate::compression_table::CompressionTable;
use crate::constants::{Bitlen, ANS_INTERLEAVING, FULL_BATCH_SIZE};
use crate::data_types::UnsignedLike;
use crate::unsigned_src_dst::DissectedLatents;
use std::cmp::min;

pub struct LatentBatchDissector<'a, U: UnsignedLike> {
  // immutable
  needs_gcd: bool,
  table: &'a CompressionTable<U>,
  encoder: &'a ans::Encoder,

  // mutable
  lower_scratch: [U; FULL_BATCH_SIZE],
  gcd_scratch: [U; FULL_BATCH_SIZE],
  token_scratch: [Token; FULL_BATCH_SIZE],
}

impl<'a, U: UnsignedLike> LatentBatchDissector<'a, U> {
  pub fn new(needs_gcd: bool, table: &'a CompressionTable<U>, encoder: &'a ans::Encoder) -> Self {
    Self {
      needs_gcd,
      table,
      encoder,
      lower_scratch: [U::ZERO; FULL_BATCH_SIZE],
      gcd_scratch: [U::ZERO; FULL_BATCH_SIZE],
      token_scratch: [0; FULL_BATCH_SIZE],
    }
  }

  #[inline(never)]
  fn binary_search(&self, latents: &[U]) -> [usize; FULL_BATCH_SIZE] {
    let mut search_idxs = [0; FULL_BATCH_SIZE];

    // we do this as `size_log` SIMD loops over the batch
    for depth in 0..self.table.search_size_log {
      let bisection_idx = 1 << (self.table.search_size_log - 1 - depth);
      for (&latent, search_idx) in latents.iter().zip(search_idxs.iter_mut()) {
        let candidate_idx = *search_idx + bisection_idx;
        let value = unsafe { *self.table.search_lowers.get_unchecked(candidate_idx) };
        *search_idx += ((latent >= value) as usize) * bisection_idx;
      }
    }

    let max_idx = self.table.infos.len().saturating_sub(1);
    search_idxs
      .iter_mut()
      .for_each(|search_idx| *search_idx = min(*search_idx, max_idx));

    search_idxs
  }

  #[inline(never)]
  fn dissect_bins(&mut self, search_idxs: &[usize], dst_offset_bits: &mut [Bitlen]) {
    for (i, &search_idx) in search_idxs.iter().enumerate() {
      let info = &self.table.infos[search_idx];
      self.lower_scratch[i] = info.lower;
      self.gcd_scratch[i] = info.gcd;
      self.token_scratch[i] = info.token;
      dst_offset_bits[i] = info.offset_bits;
    }
  }

  #[inline(never)]
  fn set_offsets(&self, latents: &[U], offsets: &mut [U]) {
    for (offset, (&latent, &lower)) in offsets
      .iter_mut()
      .zip(latents.iter().zip(self.lower_scratch.iter()))
    {
      *offset = latent - lower;
    }
  }

  #[inline(never)]
  fn divide_by_gcds(&self, offsets: &mut [U]) {
    for (offset, &gcd) in offsets.iter_mut().zip(self.gcd_scratch.iter()) {
      *offset /= gcd;
    }
  }

  #[inline(never)]
  fn encode_ans_in_reverse(
    &self,
    ans_vals: &mut [AnsState],
    ans_bits: &mut [Bitlen],
    ans_final_states: &mut [AnsState; ANS_INTERLEAVING],
  ) {
    let final_base_i = (ans_vals.len() / ANS_INTERLEAVING) * ANS_INTERLEAVING;
    let final_j = ans_vals.len() % ANS_INTERLEAVING;

    // first get the jagged part out of the way
    for j in (0..final_j).rev() {
      let i = final_base_i + j;
      let (new_state, bitlen) = self
        .encoder
        .encode(ans_final_states[j], self.token_scratch[i]);
      ans_vals[i] = ans_final_states[j];
      ans_bits[i] = bitlen;
      ans_final_states[j] = new_state;
    }

    // then do the main loop
    for base_i in (0..final_base_i).step_by(ANS_INTERLEAVING).rev() {
      for j in (0..ANS_INTERLEAVING).rev() {
        let i = base_i + j;
        let (new_state, bitlen) = self
          .encoder
          .encode(ans_final_states[j], self.token_scratch[i]);
        ans_vals[i] = ans_final_states[j];
        ans_bits[i] = bitlen;
        ans_final_states[j] = new_state;
      }
    }
  }

  pub fn dissect_latent_batch(
    &mut self,
    latents: &[U],
    base_i: usize,
    dst: &mut DissectedLatents<U>,
  ) {
    let DissectedLatents {
      ans_vals,
      ans_bits,
      offsets,
      offset_bits,
      ans_final_states,
    } = dst;

    let search_idxs = self.binary_search(latents);

    let end_i = min(base_i + FULL_BATCH_SIZE, ans_vals.len());

    self.dissect_bins(
      &search_idxs[..latents.len()],
      &mut offset_bits[base_i..end_i],
    );

    self.set_offsets(latents, &mut offsets[base_i..end_i]);

    if self.needs_gcd {
      self.divide_by_gcds(&mut offsets[base_i..end_i]);
    }

    self.encode_ans_in_reverse(
      &mut ans_vals[base_i..end_i],
      &mut ans_bits[base_i..end_i],
      ans_final_states,
    );
  }
}
