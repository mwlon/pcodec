
use std::fmt::Debug;




use crate::{ans, ChunkLatentMetadata};
use crate::ans::AnsState;
use crate::bin::BinDecompressionInfo;
use crate::bit_reader::BitReader;
use crate::chunk_metadata::{PageLatentMetadata};
use crate::constants::{Bitlen, FULL_BATCH_SIZE};
use crate::data_types::UnsignedLike;
use crate::errors::{PcoResult};





#[derive(Clone, Debug)]
struct State {
  ans_decoder: ans::Decoder,
}

pub struct Backup {
  ans_decoder_backup: AnsState,
}

impl State {
  fn backup(&self) -> Backup {
    Backup {
      ans_decoder_backup: self.ans_decoder.state,
    }
  }

  fn recover(&mut self, backup: Backup) {
    self.ans_decoder.state = backup.ans_decoder_backup;
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
    let max_bits_per_ans = chunk_latent_meta.ans_size_log - chunk_latent_meta.bins.iter().map(|bin|
    bin.weight.ilog2() as Bitlen).max().unwrap_or_default();
    let max_bits_per_offset = chunk_latent_meta.bins.iter().map(|bin| bin.offset_bits).max().unwrap_or(Bitlen::MAX);
    let infos = chunk_latent_meta.bins.iter().map(BinDecompressionInfo::from).collect::<Vec<_>>();
    let maybe_constant_value = if is_trivial {
      chunk_latent_meta.bins.first().map(|bin| bin.lower)
    } else {
      None
    };
    let ans_decoder = ans::Decoder::from_latent_meta(chunk_latent_meta, page_latent_meta.ans_final_state)?;

    Ok(Self {
      max_bits_per_ans,
      max_bits_per_offset,
      infos,
      maybe_constant_value,
      needs_gcd,
      state: State {
        ans_decoder,
      }
    })
  }

  #[inline(never)]
  fn unchecked_decompress_ans(&mut self, reader: &mut BitReader, infos: &mut [BinDecompressionInfo<U>], batch_size: usize) {
    assert!(batch_size <= infos.len());
    for i in 0..batch_size {
      infos[i] = self.infos[self.state.ans_decoder.unchecked_decode(reader) as usize];
    }
  }

  #[inline(never)]
  fn decompress_ans(&mut self, reader: &mut BitReader, infos: &mut [BinDecompressionInfo<U>], batch_size: usize) -> PcoResult<()> {
    for i in 0..batch_size {
      infos[i] = self.infos[self.state.ans_decoder.decode(reader)? as usize];
    }
    Ok(())
  }

  #[inline(never)]
  fn unchecked_decompress_offsets(&mut self, reader: &mut BitReader, infos: &[BinDecompressionInfo<U>], dst: &mut [U]) {
    assert!(dst.len() <= infos.len());
    for i in 0..dst.len() {
      dst[i] = reader.unchecked_read_uint::<U>(infos[i].offset_bits);
    }
  }

  #[inline(never)]
  fn decompress_offsets(&mut self, reader: &mut BitReader, infos: &[BinDecompressionInfo<U>], dst: &mut [U]) -> PcoResult<()> {
    for i in 0..dst.len() {
      dst[i] = reader.read_uint::<U>(infos[i].offset_bits)?;
    }
    Ok(())
  }

  #[inline(never)]
  fn multiply_by_gcds(&self, infos: &[BinDecompressionInfo<U>], dst: &mut [U]) {
    for i in 0..dst.len() {
      dst[i] *= infos[i].gcd;
    }
  }

  #[inline(never)]
  fn add_offsets(&self, infos: &[BinDecompressionInfo<U>], dst: &mut [U]) {
    for i in 0..dst.len() {
      dst[i] = dst[i].wrapping_add(infos[i].lower)
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

    let mut bin_infos: Vec<BinDecompressionInfo<U>> = unsafe {
      let mut v = Vec::with_capacity(batch_size);
      v.set_len(batch_size);
      v
    };
    assert!(dst.len() <= bin_infos.len());
    // as long as there's enough compressed data available, we don't need checked operations
    if self.max_bits_per_ans as usize * batch_size <= reader.bits_remaining() {
      self.unchecked_decompress_ans(reader, &mut bin_infos, batch_size);
    } else {
      self.decompress_ans(reader, &mut bin_infos, batch_size)?;
    }

    if self.max_bits_per_offset as usize * batch_size <= reader.bits_remaining() {
      self.unchecked_decompress_offsets(reader, &bin_infos, dst);
    } else {
      self.decompress_offsets(reader, &bin_infos, dst)?;
    }

    if self.needs_gcd {
      self.multiply_by_gcds(&bin_infos, dst);
    }

    self.add_offsets(&bin_infos, dst);

    Ok(())
  }

  pub fn backup(&self) -> Backup {
    self.state.backup()
  }

  pub fn recover(&mut self, backup: Backup) {
    self.state.recover(backup);
  }
}
