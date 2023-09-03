use std::fmt::Debug;
use std::mem;
use std::mem::MaybeUninit;

use crate::ans::{AnsState, Token};
use crate::bin::BinDecompressionInfo;
use crate::bit_reader::BitReader;
use crate::chunk_metadata::PageLatentMetadata;
use crate::constants::{ANS_INTERLEAVING, Bitlen, FULL_BATCH_SIZE, WORD_BITLEN};
use crate::data_types::UnsignedLike;
use crate::errors::PcoResult;
use crate::{ans, ChunkLatentMetadata};

#[derive(Clone, Debug)]
struct State<U: UnsignedLike> {
  // scratch needs no backup
  offset_bit_idxs_scratch: [usize; FULL_BATCH_SIZE],
  offset_bits_scratch: [Bitlen; FULL_BATCH_SIZE],
  lowers_scratch: [U; FULL_BATCH_SIZE],
  gcds_scratch: [U; FULL_BATCH_SIZE],
  state_idxs: [usize; ANS_INTERLEAVING],
}

pub struct Backup {
  state_idxs: [usize; ANS_INTERLEAVING],
}

impl<U: UnsignedLike> State<U> {
  fn backup(&self) -> Backup {
    Backup {
      state_idxs: self.state_idxs.clone(),
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
  infos: Vec<BinDecompressionInfo<U>>,
  maybe_constant_value: Option<U>,
  needs_gcd: bool,
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
      .max()
      .unwrap_or_default();
    let max_bits_per_offset = chunk_latent_meta
      .bins
      .iter()
      .map(|bin| bin.offset_bits)
      .max()
      .unwrap_or(Bitlen::MAX);
    let infos = chunk_latent_meta
      .bins
      .iter()
      .map(BinDecompressionInfo::from)
      .collect::<Vec<_>>();
    let maybe_constant_value = if is_trivial {
      chunk_latent_meta.bins.first().map(|bin| bin.lower)
    } else {
      None
    };
    let decoder =
      ans::Decoder::from_latent_meta(
        chunk_latent_meta,
      )?;

    Ok(Self {
      max_bits_per_ans,
      max_bits_per_offset,
      infos,
      maybe_constant_value,
      needs_gcd,
      decoder,
      state: State {
        offset_bit_idxs_scratch: [0; FULL_BATCH_SIZE],
        offset_bits_scratch: [0; FULL_BATCH_SIZE],
        gcds_scratch: [U::ONE; FULL_BATCH_SIZE],
        lowers_scratch: [U::ZERO; FULL_BATCH_SIZE],
        state_idxs: page_latent_meta.ans_final_state_idxs.map(|x| x as usize)
      },
    })
  }

  #[inline(never)]
  fn unchecked_decompress_ans_tokens(
    &mut self,
    reader: &mut BitReader,
  ) {
    let mut byte_idx = reader.loaded_byte_idx;
    let mut bit_idx = reader.bits_past_ptr;
    let mut offset_bit_idx = 0;
    let mut state_idxs = self.state.state_idxs.clone();
    for base_i in (0..FULL_BATCH_SIZE).step_by(4) {
      byte_idx += bit_idx as usize / 8;
      bit_idx %= 8;
      let word = reader.unchecked_word_at(byte_idx);
      for j in 0..4 {
        let i = base_i + j;
        let node = self.decoder.get_node(state_idxs[j]);
        let state_offset = (word >> bit_idx) & ((1 << node.bits_to_read) - 1);
        let info = unsafe { self.infos.get_unchecked(node.token as usize)};
        self.state.set_scratch(i, offset_bit_idx, info);
        bit_idx += node.bits_to_read;
        offset_bit_idx += info.offset_bits as usize;
        state_idxs[j] = node.next_state_idx_base + state_offset;
      }
    }
    reader.loaded_byte_idx = byte_idx;
    reader.bits_past_ptr = bit_idx;
    self.state.state_idxs = state_idxs;
    // for i in 0..batch_size {
    //   unsafe { *self.state.token_scratch.get_unchecked_mut(i) = self.state.ans_decoder.unchecked_decode(reader); }
    // }
    // for token in self.state.token_scratch.iter_mut().take(batch_size) {
    //   *token = self.state.ans_decoder.unchecked_decode(reader);
    // }
  }

  // #[inline(never)]
  // fn lookup(
  //   &mut self,
  //   infos: &mut [BinDecompressionInfo<U>],
  // ) {
  //   assert!(self.state.token_scratch.len() >= infos.len());
  //   for (i, info) in infos.iter_mut().enumerate() {
  //     *info = self.infos[self.state.token_scratch[i] as usize];
  //   }
  // }

  // #[inline(never)]
  // fn unchecked_decompress_ans(
  //   &mut self,
  //   reader: &mut BitReader,
  //   dst: &mut[U],
  //   bit_idx_dst: &mut[(Bitlen, Bitlen)],
  //   // gcds: &mut[U],
  // ) {
  //   let mut bit_idx = 0;
  //   for (x, offset_len) in dst.iter_mut().zip(bit_idx_dst.iter_mut()) {
  //     let info = &self.infos[self.state.ans_decoder.unchecked_decode(reader) as usize];
  //     *x = info.lower;
  //     *offset_len = (bit_idx, info.offset_bits);
  //     bit_idx += info.offset_bits;
  //   }
  // }

  // #[inline(never)]
  // fn decompress_ans(
  //   &mut self,
  //   reader: &mut BitReader,
  //   dst: &mut[U],
  //   bit_offset_lens: &mut[(Bitlen, Bitlen)],
  //   // gcds: &mut[U],
  // ) -> PcoResult<()> {
  //   let mut bit_idx = 0;
  //   for (x, offset_len) in dst.iter_mut().zip(bit_offset_lens.iter_mut()) {
  //     let info = &self.infos[self.state.ans_decoder.decode(reader)? as usize];
  //     *x = info.lower;
  //     *offset_len = (bit_idx, info.offset_bits);
  //     bit_idx += info.offset_bits;
  //   }
  //   Ok(())
  // }
  #[inline(never)]
  fn decompress_ans(
    &mut self,
    reader: &mut BitReader,
    batch_size: usize,
  ) -> PcoResult<()> {
    let mut state_idxs = self.state.state_idxs.clone();
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

  #[inline(never)]
  fn unchecked_decompress_offsets(
    &mut self,
    reader: &mut BitReader,
    dst: &mut [U],
  ) {
    // TODO handle multi-word offsets
    let base_bit_idx = reader.bit_idx();
    for i in 0..FULL_BATCH_SIZE {
      let bit_idx = base_bit_idx + self.state.offset_bit_idxs_scratch[i];
      let byte_idx = bit_idx / 8;
      let bits_past_byte = bit_idx % 8;
      let mut word = reader.unchecked_word_at(byte_idx);
      word >>= bits_past_byte;
      word &= (1 << self.state.offset_bits_scratch[i]) - 1;
      dst[i] = U::from_word(word);
    }
    // const NANOBATCH_SIZE: usize = 4;
    // for (dst, (bit_idxs, offset_bits)) in dst.chunks_exact_mut(NANOBATCH_SIZE).zip(self.state.offset_bit_idxs_scratch.chunks_exact_mut(NANOBATCH_SIZE).zip(self.state.offset_bits_scratch.chunks_exact(NANOBATCH_SIZE))) {
    //   for bit_idx in bit_idxs {
    //     *bit_idx += base_bit_idx;
    //   }
    //   let bits_past_byte = bit_idxs.iter().map(|&bit_idx| bit_idx as Bitlen % 8).collect::<Vec<Bitlen>>();
    //   let byte_idxs = {
    //     for bit_idx in bit_idxs {
    //       *bit_idx /= 8;
    //     }
    //     bit_idxs
    //   };
    //   let mut words = byte_idxs.iter().map(|&byte_idx| reader.unchecked_word_at(byte_idx)).collect::<[usize; NANOBATCH_SIZE]>();
    //   for (word, &bits_past_byte) in words.iter_mut().zip(bits_past_byte.iter()) {
    //     *word >>= bits_past_byte;
    //   }
    //   for (word, &offset_bits) in words.iter_mut().zip(self.state.offset_bits_scratch.iter()) {
    //     *word &= (1 << offset_bits) - 1
    //   }
    //   for (&word, x_dst) in words.iter().zip(dst.iter_mut()) {
    //     *x_dst = U::from_word(word);
    //   }
    // }
    // println!("seek to ")
    // reader.seek_to(self.state.offset_bit_idxs_scratch[FULL_BATCH_SIZE - 1] + self.state.offset_bits_scratch[FULL_BATCH_SIZE - 1] as usize)
    reader.seek_to(base_bit_idx + self.state.offset_bit_idxs_scratch[FULL_BATCH_SIZE - 1] + self.state.offset_bits_scratch[FULL_BATCH_SIZE - 1] as usize)
    // assert_eq!(bit_offset_lens.len(), dst.len());
    // let base_bit_idx = reader.bit_idx();
    // for (&(bit_offset, bit_len), x) in bit_offset_lens.iter().zip(dst.iter_mut()) {
    //   *x += reader.unchecked_peek_uint(base_bit_idx + bit_offset as usize, bit_len);
    // }
    // let &(final_bit_offset, final_bit_len) = bit_offset_lens.last().unwrap();
    // reader.seek_to(base_bit_idx + (final_bit_offset + final_bit_len) as usize);
    // assert!(dst.len() <= infos.len());
    // for i in 0..dst.len() {
    //   dst[i] = reader.unchecked_read_uint::<U>(infos[i].offset_bits);
    // }
  }

  #[inline(never)]
  fn decompress_offsets(
    &mut self,
    reader: &mut BitReader,
    dst: &mut [U],
  ) -> PcoResult<()> {
    for i in 0..dst.len() {
      dst[i] = reader.read_uint::<U>(self.state.offset_bits_scratch[i])?;
    }
    // let base_bit_idx = reader.bit_idx();
    // for (&(bit_offset, bit_len), x) in bit_offset_lens.iter().zip(dst.iter_mut()) {
    //   *x += reader.peek_uint(base_bit_idx + bit_offset as usize, bit_len)?;
    // }
    // let &(final_bit_offset, final_bit_len) = bit_offset_lens.last().unwrap();
    // reader.seek_to(base_bit_idx + (final_bit_offset + final_bit_len) as usize);
    Ok(())
  }

  #[inline(never)]
  fn multiply_by_gcds(&self, dst: &mut [U]) {
    for (&gcd, dst) in self.state.gcds_scratch[0..dst.len()].iter().zip(dst.iter_mut()) {
      *dst *= gcd;
    }
  }

  #[inline(never)]
  fn add_offsets(&self, dst: &mut [U]) {
    for (&lower, dst) in self.state.lowers_scratch[0..dst.len()].iter().zip(dst.iter_mut()) {
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

    assert!(dst.len() <= FULL_BATCH_SIZE);
    let batch_size = dst.len();
    assert!(batch_size <= FULL_BATCH_SIZE);

    // let mut bin_infos: Vec<BinDecompressionInfo<U>> = unsafe {
    //   let mut v = Vec::with_capacity(batch_size);
    //   v.set_len(batch_size);
    //   v
    // };
    // assert!(dst.len() <= bin_infos.len());
    // let mut bit_idxs = unsafe {
    //   let mut v = Vec::with_capacity(batch_size);
    //   v.set_len(batch_size);
    //   v
    // };
    // as long as there's enough compressed data available, we don't need checked operations
    if batch_size == FULL_BATCH_SIZE && self.max_bits_per_ans as usize * FULL_BATCH_SIZE <= reader.bits_remaining() {
      // self.unchecked_decompress_ans(reader, dst, &mut bit_idxs);
      self.unchecked_decompress_ans_tokens(reader);
      // self.lookup(&mut bin_infos);
    } else {
      // self.decompress_ans(reader, dst, &mut bit_idxs)?;
      self.decompress_ans(reader, batch_size)?;
      // self.lookup(&mut bin_infos);
    }

    if self.max_bits_per_offset as usize * batch_size <= reader.bits_remaining() {
      // self.unchecked_decompress_offsets(reader, &bit_idxs, dst);
      self.unchecked_decompress_offsets(reader, dst);
    } else {
      // self.decompress_offsets(reader, &bit_idxs, dst)?;
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
