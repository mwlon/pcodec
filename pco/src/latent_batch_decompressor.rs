use std::fmt::Debug;

use crate::ans::AnsState;
use crate::bin::BinDecompressionInfo;
use crate::bit_reader::BitReader;
use crate::constants::{Bitlen, ANS_INTERLEAVING, FULL_BATCH_SIZE, PAGE_PADDING};
use crate::data_types::UnsignedLike;
use crate::errors::PcoResult;
use crate::page_meta::PageLatentMeta;
use crate::{ans, bit_reader, read_write_uint, ChunkLatentMeta, Mode};

const MAX_ANS_SYMBOLS_PER_U64: usize = 4;

#[derive(Clone, Debug)]
struct State<U: UnsignedLike> {
  // scratch needs no backup
  offset_bits_csum_scratch: [usize; FULL_BATCH_SIZE],
  offset_bits_scratch: [Bitlen; FULL_BATCH_SIZE],
  lowers_scratch: [U; FULL_BATCH_SIZE],
  gcds_scratch: [U; FULL_BATCH_SIZE],
  state_idxs: [AnsState; ANS_INTERLEAVING],
}

pub struct Backup {
  state_idxs: [AnsState; ANS_INTERLEAVING],
}

impl<U: UnsignedLike> State<U> {
  fn backup(&self) -> Backup {
    Backup {
      state_idxs: self.state_idxs,
    }
  }

  fn recover(&mut self, backup: Backup) {
    self.state_idxs = backup.state_idxs;
  }

  #[inline]
  fn set_scratch(&mut self, i: usize, offset_bit_idx: usize, info: &BinDecompressionInfo<U>) {
    unsafe {
      *self.offset_bits_csum_scratch.get_unchecked_mut(i) = offset_bit_idx;
      *self.offset_bits_scratch.get_unchecked_mut(i) = info.offset_bits;
      *self.lowers_scratch.get_unchecked_mut(i) = info.lower;
      *self.gcds_scratch.get_unchecked_mut(i) = info.gcd;
    };
  }
}

// LatentBatchDecompressor does the main work of decoding bytes into UnsignedLikes
#[derive(Clone, Debug)]
pub struct LatentBatchDecompressor<U: UnsignedLike> {
  // known information about the latent latent in this chunk
  extra_u64s_per_offset: usize,
  infos: Vec<BinDecompressionInfo<U>>,
  maybe_constant_value: Option<U>,
  needs_gcd: bool,
  decoder: ans::Decoder,

  // mutable state
  state: State<U>,
}

impl<U: UnsignedLike> LatentBatchDecompressor<U> {
  pub fn new(
    chunk_latent_meta: &ChunkLatentMeta<U>,
    page_latent_meta: &PageLatentMeta<U>,
    mode: Mode<U>,
  ) -> PcoResult<Self> {
    let extra_u64s_per_offset =
      read_write_uint::calc_max_extra_u64s(chunk_latent_meta.max_bits_per_offset());
    let infos = chunk_latent_meta
      .bins
      .iter()
      .map(BinDecompressionInfo::from)
      .collect::<Vec<_>>();
    let maybe_constant_value = if chunk_latent_meta.is_trivial() {
      chunk_latent_meta.bins.first().map(|bin| bin.lower)
    } else {
      None
    };
    let decoder = ans::Decoder::from_latent_meta(chunk_latent_meta)?;

    Ok(Self {
      extra_u64s_per_offset,
      infos,
      maybe_constant_value,
      needs_gcd: chunk_latent_meta.needs_gcd(mode),
      decoder,
      state: State {
        offset_bits_csum_scratch: [0; FULL_BATCH_SIZE],
        offset_bits_scratch: [0; FULL_BATCH_SIZE],
        gcds_scratch: [U::ONE; FULL_BATCH_SIZE],
        lowers_scratch: [U::ZERO; FULL_BATCH_SIZE],
        state_idxs: page_latent_meta.ans_final_state_idxs,
      },
    })
  }

  #[allow(clippy::needless_range_loop)]
  #[inline(never)]
  fn decompress_full_ans_tokens(&mut self, reader: &mut BitReader) {
    let stream = reader.current_stream;
    let mut stale_byte_idx = reader.stale_byte_idx;
    let mut bits_past_byte = reader.bits_past_byte;
    let mut offset_bit_idx = 0;
    let mut state_idxs = self.state.state_idxs;
    // this requires that MAX_ANS_SYMBOLS_PER_U64 == ANS_INTERLEAVING
    for base_i in (0..FULL_BATCH_SIZE).step_by(MAX_ANS_SYMBOLS_PER_U64) {
      stale_byte_idx += bits_past_byte as usize / 8;
      bits_past_byte %= 8;
      let packed = bit_reader::u64_at(stream, stale_byte_idx);
      for j in 0..MAX_ANS_SYMBOLS_PER_U64 {
        let i = base_i + j;
        let node = self.decoder.get_node(state_idxs[j]);
        let state_offset = (packed >> bits_past_byte) as AnsState & ((1 << node.bits_to_read) - 1);
        let info = unsafe { self.infos.get_unchecked(node.token as usize) };
        self.state.set_scratch(i, offset_bit_idx, info);
        bits_past_byte += node.bits_to_read;
        offset_bit_idx += info.offset_bits as usize;
        state_idxs[j] = node.next_state_idx_base + state_offset;
      }
    }

    reader.stale_byte_idx = stale_byte_idx;
    reader.bits_past_byte = bits_past_byte;
    self.state.state_idxs = state_idxs;
  }

  #[inline(never)]
  fn decompress_ans_tokens(&mut self, reader: &mut BitReader, batch_size: usize) {
    let stream = reader.current_stream;
    let mut stale_byte_idx = reader.stale_byte_idx;
    let mut bits_past_byte = reader.bits_past_byte;
    let mut offset_bit_idx = 0;
    let mut state_idxs = self.state.state_idxs;
    for i in 0..batch_size {
      let j = i % 4;
      stale_byte_idx += bits_past_byte as usize / 8;
      bits_past_byte %= 8;
      let packed = bit_reader::u64_at(stream, stale_byte_idx);
      let node = self.decoder.get_node(state_idxs[j]);
      let state_offset = (packed >> bits_past_byte) as AnsState & ((1 << node.bits_to_read) - 1);
      let info = &self.infos[node.token as usize];
      self.state.set_scratch(i, offset_bit_idx, info);
      bits_past_byte += node.bits_to_read;
      offset_bit_idx += info.offset_bits as usize;
      state_idxs[j] = node.next_state_idx_base + state_offset;
    }

    reader.stale_byte_idx = stale_byte_idx;
    reader.bits_past_byte = bits_past_byte;
    self.state.state_idxs = state_idxs;
  }

  #[allow(clippy::needless_range_loop)]
  #[inline(never)]
  fn decompress_offsets<const MAX_EXTRA_U64S: usize>(
    &mut self,
    reader: &mut BitReader,
    dst: &mut [U],
  ) {
    let base_bit_idx = reader.bit_idx();
    let stream = reader.current_stream;
    for i in 0..dst.len() {
      let offset_bits = self.state.offset_bits_scratch[i];
      let bit_idx = base_bit_idx + self.state.offset_bits_csum_scratch[i];
      let byte_idx = bit_idx / 8;
      let bits_past_byte = bit_idx as Bitlen % 8;
      dst[i] = bit_reader::read_uint_at::<U, MAX_EXTRA_U64S>(
        stream,
        byte_idx,
        bits_past_byte,
        offset_bits,
      );
    }
    let final_bit_idx = base_bit_idx
      + self.state.offset_bits_csum_scratch[dst.len() - 1]
      + self.state.offset_bits_scratch[dst.len() - 1] as usize;
    reader.stale_byte_idx = final_bit_idx / 8;
    reader.bits_past_byte = final_bit_idx as Bitlen % 8;
  }

  #[inline(never)]
  fn multiply_by_gcds(&self, dst: &mut [U]) {
    for (&gcd, dst) in self.state.gcds_scratch[0..dst.len()]
      .iter()
      .zip(dst.iter_mut())
    {
      *dst *= gcd;
    }
  }

  #[inline(never)]
  fn add_offsets(&self, dst: &mut [U]) {
    for (&lower, dst) in self.state.lowers_scratch[0..dst.len()]
      .iter()
      .zip(dst.iter_mut())
    {
      *dst = dst.wrapping_add(lower);
    }
  }

  // If hits a corruption, it returns an error and leaves reader and self unchanged.
  // May contaminate dst.
  pub fn decompress_latent_batch_dirty(
    &mut self,
    reader: &mut BitReader,
    dst: &mut [U],
  ) -> PcoResult<()> {
    if dst.is_empty() {
      return Ok(());
    }

    if let Some(const_value) = self.maybe_constant_value {
      dst.fill(const_value);
      return Ok(());
    }

    let batch_size = dst.len();
    assert!(batch_size <= FULL_BATCH_SIZE);
    reader.ensure_padded(PAGE_PADDING)?;

    if batch_size == FULL_BATCH_SIZE {
      self.decompress_full_ans_tokens(reader);
    } else {
      self.decompress_ans_tokens(reader, batch_size);
    }

    // this assertion saves some unnecessary specializations in the compiled assembly
    assert!(self.extra_u64s_per_offset <= read_write_uint::calc_max_extra_u64s(U::BITS));
    match self.extra_u64s_per_offset {
      0 => self.decompress_offsets::<0>(reader, dst),
      1 => self.decompress_offsets::<1>(reader, dst),
      2 => self.decompress_offsets::<2>(reader, dst),
      _ => panic!(
        "[LatentBatchDecompressor] data type too large (extra u64's {} > 2)",
        self.extra_u64s_per_offset
      ),
    }

    if self.needs_gcd {
      self.multiply_by_gcds(dst);
    }

    self.add_offsets(dst);

    Ok(())
  }

  pub fn backup(&self) -> Backup {
    self.state.backup()
  }

  pub fn recover(&mut self, backup: Backup) {
    self.state.recover(backup);
  }
}

#[cfg(test)]
mod tests {
  use crate::constants::ANS_INTERLEAVING;
  use crate::latent_batch_decompressor::MAX_ANS_SYMBOLS_PER_U64;

  #[test]
  fn test_ans_constants_work_out() {
    assert_eq!(MAX_ANS_SYMBOLS_PER_U64, ANS_INTERLEAVING);
  }
}
