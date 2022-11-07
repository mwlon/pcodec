use crate::chunk_metadata::DataPagingSpec;
use crate::errors::{QCompressError, QCompressResult};

/// A specification for how many elements there will be in each of a chunk's
/// data pages.
///
/// By default this specifies a single data page containing all the data.
/// You can also specify exact data page sizes via
/// [`.with_page_sizes`][Self::with_page_sizes].
/// Data pages must be specified up-front for each chunk for performance
/// reasons.
#[derive(Clone, Debug, Default)]
pub struct ChunkSpec {
  data_paging_spec: DataPagingSpec,
}

impl ChunkSpec {
  /// Modifies the spec to use the exact data page sizes given. These must
  /// sum to the actual number of elements to be compressed.
  ///
  /// E.g.
  /// ```
  /// use q_compress::wrapped::ChunkSpec;
  /// let spec = ChunkSpec::default().with_page_sizes(vec![1, 2, 3]);
  /// ```
  /// can only be used if the chunk actually contains 1+2+3=6 numbers.
  pub fn with_page_sizes(mut self, sizes: Vec<usize>) -> Self {
    self.data_paging_spec = DataPagingSpec::ExactPageSizes(sizes);
    self
  }

  pub(crate) fn page_sizes(&self, n: usize) -> QCompressResult<Vec<usize>> {
    let page_sizes = match &self.data_paging_spec {
      DataPagingSpec::SinglePage => Ok(vec![n]),
      DataPagingSpec::ExactPageSizes(sizes) => {
        let sizes_n: usize = sizes.iter().sum();
        if sizes_n == n {
          Ok(sizes.clone())
        } else {
          Err(QCompressError::invalid_argument(format!(
            "chunk spec suggests {} numbers but {} were given",
            sizes_n,
            n,
          )))
        }
      }
    }?;

    for &size in &page_sizes {
      if size == 0 {
        return Err(QCompressError::invalid_argument("cannot write data page of 0 numbers"));
      }
    }

    Ok(page_sizes)
  }
}
