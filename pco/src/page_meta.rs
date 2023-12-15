use std::io::Write;

use crate::ans::AnsState;
use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::constants::{Bitlen, ANS_INTERLEAVING};
use crate::data_types::UnsignedLike;
use crate::delta::DeltaMoments;
use crate::errors::PcoResult;
use crate::ChunkMeta;

#[derive(Clone, Debug)]
pub struct PageLatentVarMeta<U: UnsignedLike> {
  pub delta_moments: DeltaMoments<U>,
  pub ans_final_state_idxs: [AnsState; ANS_INTERLEAVING],
}

impl<U: UnsignedLike> PageLatentVarMeta<U> {
  pub fn write_to<W: Write>(&self, ans_size_log: Bitlen, writer: &mut BitWriter<W>) {
    self.delta_moments.write_to(writer);

    // write the final ANS state, moving it down the range [0, table_size)
    for state_idx in self.ans_final_state_idxs {
      writer.write_uint(state_idx, ans_size_log);
    }
  }

  pub fn parse_from(
    reader: &mut BitReader,
    delta_order: usize,
    ans_size_log: Bitlen,
  ) -> PcoResult<Self> {
    let delta_moments = DeltaMoments::parse_from(reader, delta_order)?;
    let mut ans_final_state_idxs = [0; ANS_INTERLEAVING];
    for state in &mut ans_final_state_idxs {
      *state = reader.read_uint::<AnsState>(ans_size_log);
    }
    Ok(Self {
      delta_moments,
      ans_final_state_idxs,
    })
  }
}

// Data page metadata is slightly semantically different from chunk metadata,
// so it gets its own type.
// Importantly, `n` and `compressed_body_size` might come from either the
// chunk metadata parsing step (standalone mode) OR from the wrapping format
// (wrapped mode).
#[derive(Clone, Debug)]
pub struct PageMeta<U: UnsignedLike> {
  pub per_var: Vec<PageLatentVarMeta<U>>,
}

impl<U: UnsignedLike> PageMeta<U> {
  pub fn write_to<I: Iterator<Item = Bitlen>, W: Write>(
    &self,
    ans_size_logs: I,
    writer: &mut BitWriter<W>,
  ) {
    for (latent_idx, ans_size_log) in ans_size_logs.enumerate() {
      self.per_var[latent_idx].write_to(ans_size_log, writer);
    }
    writer.finish_byte();
  }

  pub fn parse_from(reader: &mut BitReader, chunk_meta: &ChunkMeta<U>) -> PcoResult<Self> {
    let mut per_var = Vec::with_capacity(chunk_meta.per_latent_var.len());
    for (latent_idx, chunk_latent_var_meta) in chunk_meta.per_latent_var.iter().enumerate() {
      per_var.push(PageLatentVarMeta::parse_from(
        reader,
        chunk_meta.delta_order_for_latent_var(latent_idx),
        chunk_latent_var_meta.ans_size_log,
      )?);
    }
    reader.drain_empty_byte("non-zero bits at end of data page metadata")?;

    Ok(Self { per_var })
  }
}
