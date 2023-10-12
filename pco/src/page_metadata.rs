use crate::ans::AnsState;
use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::ChunkMetadata;
use crate::constants::{ANS_INTERLEAVING, Bitlen};
use crate::data_types::UnsignedLike;
use crate::delta::DeltaMoments;
use crate::errors::PcoResult;

#[derive(Clone, Debug)]
pub struct PageLatentMetadata<U: UnsignedLike> {
  pub delta_moments: DeltaMoments<U>,
  pub ans_final_state_idxs: [AnsState; ANS_INTERLEAVING],
}

impl<U: UnsignedLike> PageLatentMetadata<U> {
  pub fn write_to(&self, ans_size_log: Bitlen, writer: &mut BitWriter) {
    self.delta_moments.write_to(writer);

    // write the final ANS state, moving it down the range [0, table_size)
    for state_idx in self.ans_final_state_idxs {
      writer.write_diff(state_idx, ans_size_log);
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
      *state = reader.read_uint::<AnsState>(ans_size_log)?;
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
pub struct PageMetadata<U: UnsignedLike> {
  pub latents: Vec<PageLatentMetadata<U>>,
}

impl<U: UnsignedLike> PageMetadata<U> {
  pub fn write_to<I: Iterator<Item = Bitlen>>(&self, ans_size_logs: I, writer: &mut BitWriter) {
    for (latent_idx, ans_size_log) in ans_size_logs.enumerate() {
      self.latents[latent_idx].write_to(ans_size_log, writer);
    }
    writer.finish_byte();
  }

  pub fn parse_from(reader: &mut BitReader, chunk_meta: &ChunkMetadata<U>) -> PcoResult<Self> {
    let mut latents = Vec::with_capacity(chunk_meta.latents.len());
    for (latent_idx, latent_meta) in chunk_meta.latents.iter().enumerate() {
      latents.push(PageLatentMetadata::parse_from(
        reader,
        chunk_meta.latent_delta_order(latent_idx),
        latent_meta.ans_size_log,
      )?);
    }
    reader.drain_empty_byte("non-zero bits at end of data page metadata")?;

    Ok(Self { latents })
  }
}
