use std::io::Write;

use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::constants::Bitlen;
use crate::data_types::Latent;
use crate::errors::PcoResult;
use crate::metadata::page_latent_var::PageLatentVarMeta;
use crate::metadata::ChunkMeta;

// Data page metadata is slightly semantically different from chunk metadata,
// so it gets its own type.
// Importantly, `n` and `compressed_body_size` might come from either the
// chunk metadata parsing step (standalone mode) OR from the wrapping format
// (wrapped mode).
#[derive(Clone, Debug)]
pub struct PageMeta {
  pub per_latent_var: Vec<PageLatentVarMeta>,
}

impl PageMeta {
  pub unsafe fn write_to<I: Iterator<Item = Bitlen>, W: Write>(
    &self,
    ans_size_logs: I,
    writer: &mut BitWriter<W>,
  ) {
    for (latent_idx, ans_size_log) in ans_size_logs.enumerate() {
      self.per_latent_var[latent_idx].write_to(ans_size_log, writer);
    }
    writer.finish_byte();
  }

  pub unsafe fn read_from<L: Latent>(
    reader: &mut BitReader,
    chunk_meta: &ChunkMeta,
  ) -> PcoResult<Self> {
    let mut per_latent_var = Vec::with_capacity(chunk_meta.per_latent_var.len());
    for (latent_idx, chunk_latent_var_meta) in chunk_meta.per_latent_var.iter().enumerate() {
      per_latent_var.push(PageLatentVarMeta::read_from::<L>(
        reader,
        chunk_meta
          .delta_encoding_for_latent_var(latent_idx)
          .n_latents_per_state(),
        chunk_latent_var_meta.ans_size_log,
      )?);
    }
    reader.drain_empty_byte("non-zero bits at end of data page metadata")?;

    Ok(Self { per_latent_var })
  }
}
