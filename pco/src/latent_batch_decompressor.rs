use std::cmp::min;
use std::fmt::Debug;

use crate::ans::{AnsState, Token};
use crate::bin::BinDecompressionInfo;
use crate::bit_reader::BitReader;
use crate::chunk_metadata::PageLatentMetadata;
use crate::constants::{Bitlen, ANS_INTERLEAVING, BYTES_PER_WORD, FULL_BATCH_SIZE, WORD_BITLEN, WORD_SIZE, MAX_LOOKBACK};
use crate::data_types::UnsignedLike;
use crate::errors::PcoResult;
use crate::{ans, bits, ChunkLatentMetadata};
use crate::lookback::Lookback;

#[derive(Clone, Debug)]
struct State<U: UnsignedLike> {
  // scratch needs no backup
  offset_bit_idxs_scratch: [usize; FULL_BATCH_SIZE],
  offset_bits_scratch: [Bitlen; FULL_BATCH_SIZE],
  lowers_scratch: [U; FULL_BATCH_SIZE],
  gcds_scratch: [U; FULL_BATCH_SIZE],
  recent_bin_idxs: [Token; MAX_LOOKBACK],
  state_idxs: [AnsState; ANS_INTERLEAVING],
  lookback_idx: usize,
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
      *self.offset_bit_idxs_scratch.get_unchecked_mut(i) = offset_bit_idx;
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
  max_bits_per_ans: Bitlen,
  max_bits_per_offset: Bitlen,
  extra_words_per_offset: usize,
  infos: Vec<BinDecompressionInfo<U>>,
  lookbacks: Vec<Lookback>,
  maybe_constant_value: Option<U>,
  needs_gcd: bool,
  needs_lookback: bool,
  decoder: ans::Decoder,

  // mutable state
  state: State<U>,
}

impl<U: UnsignedLike> LatentBatchDecompressor<U> {
  pub fn new(
    chunk_latent_meta: &ChunkLatentMetadata<U>,
    page_latent_meta: &PageLatentMetadata<U>,
    needs_gcd: bool,
    is_trivial: bool,
  ) -> PcoResult<Self> {
    let max_bits_per_ans = chunk_latent_meta.ans_size_log
      - chunk_latent_meta
        .bins
        .iter()
        .map(|bin| bin.weight.ilog2() as Bitlen)
        .min()
        .unwrap_or_default();
    let max_bits_per_offset = chunk_latent_meta
      .bins
      .iter()
      .map(|bin| bin.offset_bits)
      .max()
      .unwrap_or(Bitlen::MAX);
    let extra_words_per_offset = ((max_bits_per_offset.saturating_add(7)) / WORD_BITLEN) as usize;
    let infos = chunk_latent_meta
      .bins
      .iter()
      .map(BinDecompressionInfo::from)
      .collect::<Vec<_>>();
    let lookbacks = chunk_latent_meta
      .lookbacks
      .iter()
      .map(|lookback_meta| lookback_meta.lookback)
      .collect::<Vec<_>>();
    let maybe_constant_value = if is_trivial {
      chunk_latent_meta.bins.first().map(|bin| bin.lower)
    } else {
      None
    };
    let needs_lookback = !chunk_latent_meta.lookbacks.is_empty();
    let decoder = ans::Decoder::from_latent_meta(chunk_latent_meta)?;

    Ok(Self {
      max_bits_per_ans,
      max_bits_per_offset,
      extra_words_per_offset,
      infos,
      lookbacks,
      maybe_constant_value,
      needs_gcd,
      needs_lookback,
      decoder,
      state: State {
        offset_bit_idxs_scratch: [0; FULL_BATCH_SIZE],
        offset_bits_scratch: [0; FULL_BATCH_SIZE],
        gcds_scratch: [U::ONE; FULL_BATCH_SIZE],
        lowers_scratch: [U::ZERO; FULL_BATCH_SIZE],
        recent_bin_idxs: [0; MAX_LOOKBACK],
        state_idxs: page_latent_meta.ans_final_state_idxs,
        lookback_idx: MAX_LOOKBACK,
      },
    })
  }

  #[allow(clippy::needless_range_loop)]
  #[inline(never)]
  fn unchecked_decompress_ans_tokens<const USE_LOOKBACK: bool>(&mut self, reader: &mut BitReader) {
    let mut byte_idx = reader.loaded_byte_idx;
    let mut bit_idx = reader.bits_past_ptr;
    let mut offset_bit_idx = 0;
    let mut state_idxs = self.state.state_idxs;
    let n_infos = self.infos.len() as Token;
    let base_lookback_idx = self.state.lookback_idx;

    for base_i in (0..FULL_BATCH_SIZE).step_by(4) {
      byte_idx += bit_idx as usize / 8;
      bit_idx %= 8;
      let word = reader.unchecked_word_at(byte_idx);
      for j in 0..4 {
        let i = base_i + j;
        let node = self.decoder.get_node(state_idxs[j]);
        let state_offset = (word >> bit_idx) as AnsState & ((1 << node.bits_to_read) - 1);
        let token = node.token;
        let bin_idx = if USE_LOOKBACK && token >= n_infos {
          let lookback = self.lookbacks[(token - n_infos) as usize];
          self.state.recent_bin_idxs[(base_lookback_idx + i - lookback as usize) % MAX_LOOKBACK]
        } else {
          token
        };
        let info = unsafe { self.infos.get_unchecked(bin_idx as usize) };
        if USE_LOOKBACK {
          self.state.recent_bin_idxs[(base_lookback_idx + i) % MAX_LOOKBACK] = bin_idx;
        }
        self.state.set_scratch(i, offset_bit_idx, info);
        bit_idx += node.bits_to_read;
        offset_bit_idx += info.offset_bits as usize;
        state_idxs[j] = node.next_state_idx_base + state_offset;
      }
    }
    reader.loaded_byte_idx = byte_idx;
    reader.bits_past_ptr = bit_idx;
    self.state.state_idxs = state_idxs;
    self.state.lookback_idx += FULL_BATCH_SIZE;
  }

  #[inline(never)]
  fn decompress_ans(&mut self, reader: &mut BitReader, batch_size: usize) -> PcoResult<()> {
    let mut state_idxs = self.state.state_idxs;
    let mut offset_bit_idx = 0;
    for i in 0..batch_size {
      let j = i % 4;
      let node = self.decoder.get_node(state_idxs[j]);
      let state_offset = reader.read_small(node.bits_to_read)?;
      let info = &self.infos[node.token as usize];
      self.state.set_scratch(i, offset_bit_idx, info);
      offset_bit_idx += info.offset_bits as usize;
      state_idxs[j] = node.next_state_idx_base + state_offset;
    }
    self.state.state_idxs = state_idxs;
    Ok(())
  }

  #[allow(clippy::needless_range_loop)]
  #[inline(never)]
  fn unchecked_decompress_offsets<const MAX_EXTRA_WORDS: usize>(
    &mut self,
    reader: &mut BitReader,
    dst: &mut [U],
  ) {
    let base_bit_idx = reader.bit_idx();
    for i in 0..FULL_BATCH_SIZE {
      let offset_bits = self.state.offset_bits_scratch[i];
      let bit_idx = base_bit_idx + self.state.offset_bit_idxs_scratch[i];
      let bits_past_byte = bit_idx as Bitlen % 8;
      let mut byte_idx = bit_idx / 8;
      let mut res = U::from_word(reader.unchecked_word_at(byte_idx) >> bits_past_byte);
      let mut processed = min(offset_bits, WORD_BITLEN - 8 - bits_past_byte);
      byte_idx += BYTES_PER_WORD - 1;

      for _ in 0..MAX_EXTRA_WORDS {
        res |= U::from_word(reader.unchecked_word_at(byte_idx)) << processed;
        processed = min(offset_bits, processed + WORD_BITLEN);
        byte_idx += BYTES_PER_WORD;
      }

      dst[i] = bits::lowest_bits(res, offset_bits);
    }
    reader.seek_to(
      base_bit_idx
        + self.state.offset_bit_idxs_scratch[FULL_BATCH_SIZE - 1]
        + self.state.offset_bits_scratch[FULL_BATCH_SIZE - 1] as usize,
    )
  }

  #[inline(never)]
  fn decompress_offsets(&mut self, reader: &mut BitReader, dst: &mut [U]) -> PcoResult<()> {
    for (i, x_dst) in dst.iter_mut().enumerate() {
      *x_dst = reader.read_uint::<U>(self.state.offset_bits_scratch[i])?;
    }
    Ok(())
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
    if let Some(const_value) = self.maybe_constant_value {
      dst.fill(const_value);
      return Ok(());
    }

    let batch_size = dst.len();
    assert!(batch_size <= FULL_BATCH_SIZE);

    // as long as there's enough compressed data available, we don't need checked operations
    let is_full_batch = batch_size == FULL_BATCH_SIZE;
    if is_full_batch && self.max_bits_per_ans as usize * FULL_BATCH_SIZE <= reader.bits_remaining()
    {
      if self.needs_lookback {
        self.unchecked_decompress_ans_tokens::<true>(reader);
      } else {
        self.unchecked_decompress_ans_tokens::<false>(reader);
      }
    } else {
      self.decompress_ans(reader, batch_size)?;
    }

    if is_full_batch
      && self.max_bits_per_offset as usize * FULL_BATCH_SIZE <= reader.bits_remaining()
    {
      // this assertion saves some unnecessary specializations in the compiled assembly
      assert!(self.extra_words_per_offset <= (U::PHYSICAL_BITS + 8) / WORD_SIZE);
      match self.extra_words_per_offset {
        0 => self.unchecked_decompress_offsets::<0>(reader, dst),
        1 => self.unchecked_decompress_offsets::<1>(reader, dst),
        2 => self.unchecked_decompress_offsets::<2>(reader, dst),
        _ => panic!("invalid extra words per offset; a bug in pcodec"),
      }
    } else {
      self.decompress_offsets(reader, dst)?;
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
