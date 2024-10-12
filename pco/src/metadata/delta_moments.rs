use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::data_types::Latent;
use std::io::Write;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeltaMoments<L: Latent>(pub Vec<L>);

impl<L: Latent> DeltaMoments<L> {
  pub unsafe fn read_from(reader: &mut BitReader, order: usize) -> Self {
    let mut moments = Vec::new();
    for _ in 0..order {
      moments.push(reader.read_uint::<L>(L::BITS));
    }
    DeltaMoments(moments)
  }

  pub unsafe fn write_to<W: Write>(&self, writer: &mut BitWriter<W>) {
    for &moment in &self.0 {
      writer.write_uint(moment, L::BITS);
    }
  }

  pub fn order(&self) -> usize {
    self.0.len()
  }
}
