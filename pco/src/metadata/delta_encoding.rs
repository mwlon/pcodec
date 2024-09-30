use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::constants::{Bitlen, Lookback};
use crate::data_types::Latent;
use crate::errors::PcoResult;
use crate::ChunkVarMeta;
use std::io::Write;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum DeltaEncoding {
  #[default]
  None,
  Consecutive {
    order: usize,
  },
  Lz(ChunkVarMeta<Lookback>),
}

impl DeltaEncoding {
  // pub(crate) fn n_lookback_vars(&self) -> usize {
  //   match self {
  //     Self::None => 0,
  //     Self::Consecutive { order: _ } => 0,
  //     Self::Lz(_) => 1,
  //   }
  // }

  pub(crate) fn n_delta_moments(&self) -> usize {
    match self {
      Self::None => 0,
      Self::Consecutive(order) => *order,
      Self::Lz(_) => 0,
    }
  }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeltaMoments<L: Latent> {
  // length = consecutive delta encoding order
  pub moments: Vec<L>,
}

impl<L: Latent> DeltaMoments<L> {
  pub(crate) fn new(moments: Vec<L>) -> Self {
    Self { moments }
  }

  pub unsafe fn parse_from(reader: &mut BitReader, order: usize) -> PcoResult<Self> {
    let mut moments = Vec::new();
    for _ in 0..order {
      moments.push(reader.read_uint::<L>(L::BITS));
    }
    Ok(DeltaMoments { moments })
  }

  pub unsafe fn write_to<W: Write>(&self, writer: &mut BitWriter<W>) {
    for &moment in &self.moments {
      writer.write_uint(moment, L::BITS);
    }
  }

  pub fn order(&self) -> usize {
    self.moments.len()
  }
}
