use std::fmt::Debug;

use crate::ans::{AnsState, Token};
use crate::bin::BinDecompressionInfo;
use crate::bit_reader::BitReader;
use crate::chunk_metadata::PageLatentMetadata;
use crate::constants::{Bitlen, FULL_BATCH_SIZE};
use crate::data_types::UnsignedLike;
use crate::errors::PcoResult;
use crate::{ans, ChunkLatentMetadata};

#[derive(Clone, Debug)]
struct State {
  token_scratch: [Token; FULL_BATCH_SIZE], // needs no backup
  ans_decoder: ans::Decoder,
}

pub struct Backup {
  ans_decoder_backup: AnsState,
}

impl State {
  fn backup(&self) -> Backup {
    Backup {
      ans_decoder_backup: self.ans_decoder.state(),
    }
  }

  fn recover(&mut self, backup: Backup) {
    self.ans_decoder.recover(backup.ans_decoder_backup);
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

  // mutable state
  state: State,
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
    let ans_decoder = ans::Decoder::from_latent_meta(
      chunk_latent_meta,
      page_latent_meta.ans_final_state,
    )?;

    Ok(Self {
      max_bits_per_ans,
      max_bits_per_offset,
      infos,
      maybe_constant_value,
      needs_gcd,
      state: State {
        ans_decoder,
        token_scratch: [0; FULL_BATCH_SIZE],
      },
    })
  }

  // #[inline(never)]
  // fn unchecked_decompress_ans_tokens(
  //   &mut self,
  //   reader: &mut BitReader,
  //   batch_size: usize,
  // ) {
  //   assert!(batch_size <= FULL_BATCH_SIZE);
  //   for token in self.state.token_scratch.iter_mut().take(batch_size) {
  //     *token = self.state.ans_decoder.unchecked_decode(reader);
  //   }
  // }

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
  //
  #[inline(never)]
  fn unchecked_decompress_ans(
    &mut self,
    reader: &mut BitReader,
    dst: &mut[U],
    bit_idx_dst: &mut[(Bitlen, Bitlen)],
    // gcds: &mut[U],
  ) {
    let mut bit_idx = 0;
    for (x, offset_len) in dst.iter_mut().zip(bit_idx_dst.iter_mut()) {
      let info = &self.infos[self.state.ans_decoder.unchecked_decode(reader) as usize];
      *x = info.lower;
      *offset_len = (bit_idx, info.offset_bits);
      bit_idx += info.offset_bits;
    }
  }

  #[inline(never)]
  fn decompress_ans(
    &mut self,
    reader: &mut BitReader,
    dst: &mut[U],
    bit_offset_lens: &mut[(Bitlen, Bitlen)],
    // gcds: &mut[U],
  ) -> PcoResult<()> {
    let mut bit_idx = 0;
    for (x, offset_len) in dst.iter_mut().zip(bit_offset_lens.iter_mut()) {
      let info = &self.infos[self.state.ans_decoder.decode(reader)? as usize];
      *x = info.lower;
      *offset_len = (bit_idx, info.offset_bits);
      bit_idx += info.offset_bits;
    }
    Ok(())
  }
  // #[inline(never)]
  // fn decompress_ans(
  //   &mut self,
  //   reader: &mut BitReader,
  //   infos: &mut [BinDecompressionInfo<U>],
  // ) -> PcoResult<()> {
  //   for info in infos.iter_mut() {
  //     *info = self.infos[self.state.ans_decoder.decode(reader)? as usize];
  //   }
  //   Ok(())
  // }

  #[inline(never)]
  fn unchecked_decompress_offsets(
    &mut self,
    reader: &mut BitReader,
    bit_offset_lens: &[(Bitlen, Bitlen)],
    // infos: &[BinDecompressionInfo<U>],
    dst: &mut [U],
  ) {
    assert_eq!(bit_offset_lens.len(), dst.len());
    let base_bit_idx = reader.bit_idx();
    for (&(bit_offset, bit_len), x) in bit_offset_lens.iter().zip(dst.iter_mut()) {
      *x += reader.unchecked_peek_uint(base_bit_idx + bit_offset as usize, bit_len);
    }
    let &(final_bit_offset, final_bit_len) = bit_offset_lens.last().unwrap();
    reader.seek_to(base_bit_idx + (final_bit_offset + final_bit_len) as usize);
    // assert!(dst.len() <= infos.len());
    // for i in 0..dst.len() {
    //   dst[i] = reader.unchecked_read_uint::<U>(infos[i].offset_bits);
    // }
  }

  #[inline(never)]
  fn decompress_offsets(
    &mut self,
    reader: &mut BitReader,
    bit_offset_lens: &[(Bitlen, Bitlen)],
    // infos: &[BinDecompressionInfo<U>],
    dst: &mut [U],
  ) -> PcoResult<()> {
    // for i in 0..dst.len() {
    //   dst[i] = reader.read_uint::<U>(infos[i].offset_bits)?;
    // }
    let base_bit_idx = reader.bit_idx();
    for (&(bit_offset, bit_len), x) in bit_offset_lens.iter().zip(dst.iter_mut()) {
      *x += reader.peek_uint(base_bit_idx + bit_offset as usize, bit_len)?;
    }
    let &(final_bit_offset, final_bit_len) = bit_offset_lens.last().unwrap();
    reader.seek_to(base_bit_idx + (final_bit_offset + final_bit_len) as usize);
    Ok(())
  }

  #[inline(never)]
  fn multiply_by_gcds(&self, infos: &[BinDecompressionInfo<U>], dst: &mut [U]) {
    for (info, dst) in infos.into_iter().zip(dst.iter_mut()) {
      *dst *= info.gcd;
    }
  }

  #[inline(never)]
  fn add_offsets(&self, infos: &[BinDecompressionInfo<U>], dst: &mut [U]) {
    for (info, dst) in infos.into_iter().zip(dst.iter_mut()) {
      *dst = dst.wrapping_add(info.lower);
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
    let mut bit_idxs = unsafe {
      let mut v = Vec::with_capacity(batch_size);
      v.set_len(batch_size);
      v
    };
    // as long as there's enough compressed data available, we don't need checked operations
    if self.max_bits_per_ans as usize * batch_size <= reader.bits_remaining() {
      self.unchecked_decompress_ans(reader, dst, &mut bit_idxs);
      // self.unchecked_decompress_ans_tokens(reader, batch_size);
      // self.lookup(&mut bin_infos);
    } else {
      self.decompress_ans(reader, dst, &mut bit_idxs)?;
    }

    if self.max_bits_per_offset as usize * batch_size <= reader.bits_remaining() {
      self.unchecked_decompress_offsets(reader, &bit_idxs, dst);
    } else {
      self.decompress_offsets(reader, &bit_idxs, dst)?;
    }

    // if self.needs_gcd {
    //   self.multiply_by_gcds(&bin_infos, dst);
    // }
    //
    // self.add_offsets(&bin_infos, dst);

    Ok(())
  }

  pub fn backup(&self) -> Backup {
    self.state.backup()
  }

  pub fn recover(&mut self, backup: Backup) {
    self.state.recover(backup);
  }
}
