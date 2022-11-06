use crate::bit_writer::BitWriter;
use crate::data_types::NumberLike;
use crate::delta_encoding::DeltaMoments;

// only used internally
#[derive(Clone, Debug, PartialEq)]
pub struct DataPageMetadata<T: NumberLike> {
  delta_moments: DeltaMoments<T::Signed>,
  pub(crate) n: usize, // not available in wrapped decompression
}

impl<T: NumberLike> DataPageMetadata<T> {
  pub(crate) fn new(delta_moments: DeltaMoments<T::Signed>, n: usize) -> Self {
    DataPageMetadata { delta_moments, n }
  }

  pub(crate) fn write_to(&self, writer: &mut BitWriter) {
    self.delta_moments.write_to(writer);
  }
}
