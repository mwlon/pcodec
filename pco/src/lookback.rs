use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::constants::{Bitlen, BITS_TO_ENCODE_LOOKBACK};

use crate::errors::PcoResult;

pub type Lookback = u32;

#[derive(Clone, Debug, PartialEq)]
pub struct LookbackMetadata {
  pub weight: usize,
  pub lookback: Lookback,
}

impl LookbackMetadata {
  pub(crate) fn write_to(&self, ans_size_log: Bitlen, writer: &mut BitWriter) {
    writer.write_usize(self.weight - 1, ans_size_log);
    writer.write_diff(self.lookback - 1, BITS_TO_ENCODE_LOOKBACK);
  }

  pub(crate) fn parse_from(reader: &mut BitReader, ans_size_log: Bitlen) -> PcoResult<Self> {
    let weight = reader.read_usize(ans_size_log)? + 1;
    let lookback = reader.read_uint::<u32>(BITS_TO_ENCODE_LOOKBACK)? + 1;

    Ok(LookbackMetadata { weight, lookback })
  }
}
