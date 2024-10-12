use crate::ans::AnsState;
use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::constants::{Bitlen, ANS_INTERLEAVING};
use crate::data_types::Latent;
use crate::errors::PcoResult;
use crate::metadata::delta_moments::DeltaMoments;
use std::io::Write;

#[derive(Clone, Debug)]
pub struct PageLatentVarMeta<L: Latent> {
  pub delta_moments: DeltaMoments<L>,
  pub ans_final_state_idxs: [AnsState; ANS_INTERLEAVING],
}

impl<L: Latent> PageLatentVarMeta<L> {
  pub unsafe fn write_to<W: Write>(&self, ans_size_log: Bitlen, writer: &mut BitWriter<W>) {
    self.delta_moments.write_to(writer);

    // write the final ANS state, moving it down the range [0, table_size)
    for state_idx in self.ans_final_state_idxs {
      writer.write_uint(state_idx, ans_size_log);
    }
  }

  pub unsafe fn read_from(
    reader: &mut BitReader,
    delta_order: usize,
    ans_size_log: Bitlen,
  ) -> PcoResult<Self> {
    let delta_moments = DeltaMoments::read_from(reader, delta_order);
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
