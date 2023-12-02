use std::fmt::Debug;

use crate::ans::AnsState;
use crate::bin::BinDecompressionInfo;
use crate::bit_reader::BitReader;
use crate::constants::{Bitlen, ANS_INTERLEAVING, FULL_BATCH_N};
use crate::data_types::UnsignedLike;
use crate::errors::PcoResult;
use crate::page_meta::PageLatentVarMeta;
use crate::{ans, bit_reader, read_write_uint, ChunkLatentVarMeta};

#[derive(Clone, Debug)]
struct State<U: UnsignedLike> {
  // scratch needs no backup
  offset_bits_csum_scratch: [usize; FULL_BATCH_N],
  offset_bits_scratch: [Bitlen; FULL_BATCH_N],
  lowers_scratch: [U; FULL_BATCH_N],
  state_idxs: [AnsState; ANS_INTERLEAVING],
}

impl<U: UnsignedLike> State<U> {
  #[inline]
  fn set_scratch(&mut self, i: usize, offset_bit_idx: usize, info: &BinDecompressionInfo<U>) {
    unsafe {
      *self.offset_bits_csum_scratch.get_unchecked_mut(i) = offset_bit_idx;
      *self.offset_bits_scratch.get_unchecked_mut(i) = info.offset_bits;
      *self.lowers_scratch.get_unchecked_mut(i) = info.lower;
    };
  }
}

// LatentBatchDecompressor does the main work of decoding bytes into UnsignedLikes
#[derive(Clone, Debug)]
pub struct LatentBatchDecompressor<U: UnsignedLike> {
  // known information about this latent variable
  u64s_per_offset: usize,
  infos: Vec<BinDecompressionInfo<U>>,
  needs_ans: bool,
  decoder: ans::Decoder,
  pub maybe_constant_value: Option<U>,

  // mutable state
  state: State<U>,
}

impl<U: UnsignedLike> LatentBatchDecompressor<U> {
  pub fn new(
    chunk_latent_var_meta: &ChunkLatentVarMeta<U>,
    page_latent_var_meta: &PageLatentVarMeta<U>,
  ) -> PcoResult<Self> {
    let u64s_per_offset =
      read_write_uint::calc_max_u64s(chunk_latent_var_meta.max_bits_per_offset());
    let infos = chunk_latent_var_meta
      .bins
      .iter()
      .map(BinDecompressionInfo::from)
      .collect::<Vec<_>>();
    let decoder = ans::Decoder::from_chunk_latent_var_meta(chunk_latent_var_meta)?;

    let mut state = State {
      offset_bits_csum_scratch: [0; FULL_BATCH_N],
      offset_bits_scratch: [0; FULL_BATCH_N],
      lowers_scratch: [U::ZERO; FULL_BATCH_N],
      state_idxs: page_latent_var_meta.ans_final_state_idxs,
    };

    let needs_ans = chunk_latent_var_meta.bins.len() != 1;
    if !needs_ans {
      // we optimize performance by setting state once and never again
      let bin = &chunk_latent_var_meta.bins[0];
      let mut csum = 0;
      for i in 0..FULL_BATCH_N {
        state.offset_bits_scratch[i] = bin.offset_bits;
        state.offset_bits_csum_scratch[i] = csum;
        state.lowers_scratch[i] = bin.lower;
        csum += bin.offset_bits as usize;
      }
    }

    let maybe_constant_value = if chunk_latent_var_meta.is_trivial() {
      chunk_latent_var_meta.bins.first().map(|bin| bin.lower)
    } else {
      None
    };

    Ok(Self {
      u64s_per_offset,
      infos,
      needs_ans,
      decoder,
      maybe_constant_value,
      state,
    })
  }

  // This implementation handles only a full batch, but is faster.
  #[inline(never)]
  fn decompress_full_ans_tokens(&mut self, reader: &mut BitReader) {
    // At each iteration, this loads a single u64 and has all ANS decoders
    // read a single token from it.
    // Therefore it requires that ANS_INTERLEAVING * MAX_BITS_PER_ANS <= 57.
    // Additionally, we're unpacking all ANS states using the fact that
    // ANS_INTERLEAVING == 4.
    let src = reader.src;
    let mut stale_byte_idx = reader.stale_byte_idx;
    let mut bits_past_byte = reader.bits_past_byte;
    let mut offset_bit_idx = 0;
    let [mut state_idx_0, mut state_idx_1, mut state_idx_2, mut state_idx_3] =
      self.state.state_idxs;
    let infos = self.infos.as_slice();
    let ans_nodes = self.decoder.nodes.as_slice();
    for base_i in (0..FULL_BATCH_N).step_by(ANS_INTERLEAVING) {
      stale_byte_idx += bits_past_byte as usize / 8;
      bits_past_byte %= 8;
      let packed = bit_reader::u64_at(src, stale_byte_idx);
      // I hate that I have to do this with a macro, but it gives a serious
      // performance gain. If I use a [AnsState; 4] for the state_idxs instead
      // of separate identifiers, it tries to repeatedly load and write to
      // the array instead of keeping the states in registers.
      macro_rules! handle_single_token {
        ($j: expr, $state_idx: ident) => {
          let i = base_i + $j;
          let node = unsafe { ans_nodes.get_unchecked($state_idx as usize) };
          let ans_val = (packed >> bits_past_byte) as AnsState & ((1 << node.bits_to_read) - 1);
          let info = unsafe { infos.get_unchecked(node.token as usize) };
          self.state.set_scratch(i, offset_bit_idx, info);
          bits_past_byte += node.bits_to_read;
          offset_bit_idx += info.offset_bits as usize;
          $state_idx = node.next_state_idx_base + ans_val;
        };
      }
      handle_single_token!(0, state_idx_0);
      handle_single_token!(1, state_idx_1);
      handle_single_token!(2, state_idx_2);
      handle_single_token!(3, state_idx_3);
    }

    reader.stale_byte_idx = stale_byte_idx;
    reader.bits_past_byte = bits_past_byte;
    self.state.state_idxs = [state_idx_0, state_idx_1, state_idx_2, state_idx_3];
  }

  // This implementation handles arbitrary batch size and looks simpler, but is
  // slower, so we only use it at the end of the page.
  #[inline(never)]
  fn decompress_ans_tokens(&mut self, reader: &mut BitReader, batch_n: usize) {
    let src = reader.src;
    let mut stale_byte_idx = reader.stale_byte_idx;
    let mut bits_past_byte = reader.bits_past_byte;
    let mut offset_bit_idx = 0;
    let mut state_idxs = self.state.state_idxs;
    for i in 0..batch_n {
      let j = i % 4;
      stale_byte_idx += bits_past_byte as usize / 8;
      bits_past_byte %= 8;
      let packed = bit_reader::u64_at(src, stale_byte_idx);
      let node = unsafe { self.decoder.nodes.get_unchecked(state_idxs[j] as usize) };
      let ans_val = (packed >> bits_past_byte) as AnsState & ((1 << node.bits_to_read) - 1);
      let info = &self.infos[node.token as usize];
      self.state.set_scratch(i, offset_bit_idx, info);
      bits_past_byte += node.bits_to_read;
      offset_bit_idx += info.offset_bits as usize;
      state_idxs[j] = node.next_state_idx_base + ans_val;
    }

    reader.stale_byte_idx = stale_byte_idx;
    reader.bits_past_byte = bits_past_byte;
    self.state.state_idxs = state_idxs;
  }

  #[allow(clippy::needless_range_loop)]
  #[inline(never)]
  fn decompress_offsets<const MAX_U64S: usize>(&mut self, reader: &mut BitReader, dst: &mut [U]) {
    let base_bit_idx = reader.bit_idx();
    let src = reader.src;
    let state = &mut self.state;
    for (dst, (&offset_bits, &offset_bits_csum)) in dst.iter_mut().zip(
      state
        .offset_bits_scratch
        .iter()
        .zip(state.offset_bits_csum_scratch.iter()),
    ) {
      let bit_idx = base_bit_idx + offset_bits_csum;
      let byte_idx = bit_idx / 8;
      let bits_past_byte = bit_idx as Bitlen % 8;
      *dst = bit_reader::read_uint_at::<U, MAX_U64S>(src, byte_idx, bits_past_byte, offset_bits);
    }
    let final_bit_idx = base_bit_idx
      + state.offset_bits_csum_scratch[dst.len() - 1]
      + state.offset_bits_scratch[dst.len() - 1] as usize;
    reader.stale_byte_idx = final_bit_idx / 8;
    reader.bits_past_byte = final_bit_idx as Bitlen % 8;
  }

  #[inline(never)]
  fn add_lowers(&self, dst: &mut [U]) {
    for (&lower, dst) in self.state.lowers_scratch[0..dst.len()]
      .iter()
      .zip(dst.iter_mut())
    {
      *dst = dst.wrapping_add(lower);
    }
  }

  // If hits a corruption, it returns an error and leaves reader and self unchanged.
  // May contaminate dst.
  pub fn decompress_latent_batch(
    &mut self,
    reader: &mut BitReader,
    dst: &mut [U],
  ) -> PcoResult<()> {
    if dst.is_empty() {
      return Ok(());
    }

    if self.needs_ans {
      let batch_n = dst.len();
      assert!(batch_n <= FULL_BATCH_N);

      if batch_n == FULL_BATCH_N {
        self.decompress_full_ans_tokens(reader);
      } else {
        self.decompress_ans_tokens(reader, batch_n);
      }
    }

    // this assertion saves some unnecessary specializations in the compiled assembly
    assert!(self.u64s_per_offset <= read_write_uint::calc_max_u64s(U::BITS));
    match self.u64s_per_offset {
      0 => dst.fill(U::ZERO),
      1 => self.decompress_offsets::<1>(reader, dst),
      2 => self.decompress_offsets::<2>(reader, dst),
      3 => self.decompress_offsets::<3>(reader, dst),
      _ => panic!(
        "[LatentBatchDecompressor] data type too large (extra u64's {} > 2)",
        self.u64s_per_offset
      ),
    }

    self.add_lowers(dst);

    Ok(())
  }
}
