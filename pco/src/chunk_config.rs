use crate::constants::DEFAULT_MAX_PAGE_SIZE;
use crate::errors::{PcoError, PcoResult};
use crate::{bits, DEFAULT_COMPRESSION_LEVEL};

/// All configurations available for a compressor.
///
/// Some, like `delta_encoding_order`, are explicitly stored in the
/// compressed bytes.
/// Others, like `compression_level`, affect compression but are not explicitly
/// stored in the output.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ChunkConfig {
  /// `compression_level` ranges from 0 to 12 inclusive (default 8).
  ///
  /// The compressor uses up to 2^`compression_level` bins.
  ///
  /// For example,
  /// * Level 0 achieves a small amount of compression with 1 bin.
  /// * Level 8 achieves nearly the best compression with 256 bins and still
  /// runs in reasonable time.
  /// * Level 12 can marginally better compression than 8 with 4096
  /// bins, but may run several times slower.
  pub compression_level: usize,
  /// `delta_encoding_order` ranges from 0 to 7 inclusive (defaults to
  /// automatically detecting on each chunk).
  ///
  /// It is the number of times to apply delta encoding
  /// before compressing. For instance, say we have the numbers
  /// `[0, 2, 2, 4, 4, 6, 6]` and consider different delta encoding orders.
  /// * 0th order takes numbers as-is.
  /// This is perfect for columnar data were the order is essentially random.
  /// * 1st order takes consecutive differences, leaving
  /// `[0, 2, 0, 2, 0, 2, 0]`. This is best for continuous but noisy time
  /// series data, like stock prices or most time series data.
  /// * 2nd order takes consecutive differences again,
  /// leaving `[2, -2, 2, -2, 2, -2]`. This is best for piecewise-linear or
  /// somewhat quadratic data.
  /// * Even higher-order is best for time series that are very
  /// smooth, like temperature or light sensor readings.
  ///
  /// If you would like to automatically choose this once and reuse it for all
  /// chunks,
  /// [`auto_compressor_config()`][crate::auto_delta_encoding_order] can help.
  pub delta_encoding_order: Option<usize>,
  /// `use_gcds` improves compression ratio in cases where all
  /// numbers in a bin share a nontrivial Greatest Common Divisor
  /// (default true).
  ///
  /// Examples where this helps:
  /// * nanosecond-precision timestamps that are all whole numbers of
  /// microseconds
  /// * integers `[7, 107, 207, 307, ... 100007]` shuffled
  ///
  /// When this is helpful, compression and decompression speeds are slightly
  /// reduced (up to ~15%). In rare cases, this configuration may reduce
  /// compression speed even when it isn't helpful.
  pub use_gcds: bool,
  /// `use_float_mult` improves compression ratio in cases where the data type
  /// is a float and all numbers are close to a multiple of a single float
  /// `base`.
  /// (default true).
  ///
  /// `base` is automatically detected. For example, this is helpful if all
  /// floats are approximately decimals (multiples of 0.01).
  ///
  /// When this is helpful, compression and decompression speeds are
  /// substantially reduced (up to ~50%). In rare cases, this configuration
  /// may reduce compression speed somewhat even when it isn't helpful.
  /// However, the compression ratio improvements tend to be quite large.
  pub use_float_mult: bool,
  /// `paging_spec` specifies how the chunk should be split into pages
  ///
  /// See [`PagingSpec`][crate::PagingSpec] for more information.
  pub paging_spec: PagingSpec,
}

impl Default for ChunkConfig {
  fn default() -> Self {
    Self {
      compression_level: DEFAULT_COMPRESSION_LEVEL,
      delta_encoding_order: None,
      use_gcds: true,
      use_float_mult: true,
      paging_spec: Default::default(),
    }
  }
}

impl ChunkConfig {
  /// Sets [`compression_level`][ChunkConfig::compression_level].
  pub fn with_compression_level(mut self, level: usize) -> Self {
    self.compression_level = level;
    self
  }

  /// Sets [`delta_encoding_order`][ChunkConfig::delta_encoding_order].
  pub fn with_delta_encoding_order(mut self, order: Option<usize>) -> Self {
    self.delta_encoding_order = order;
    self
  }

  /// Sets [`use_gcds`][ChunkConfig::use_gcds].
  pub fn with_use_gcds(mut self, use_gcds: bool) -> Self {
    self.use_gcds = use_gcds;
    self
  }

  /// Sets [`paging_spec`][ChunkConfig::paging_spec].
  pub fn with_paging_spec(mut self, paging_spec: PagingSpec) -> Self {
    self.paging_spec = paging_spec;
    self
  }
}

/// `PagingSpec` specifies how a chunk is split into pages.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum PagingSpec {
  /// Divide the chunk into equal pages of up to the provided size in numbers.
  ///
  /// For example, with a default size of 100,000, a chunk of size 150,000
  /// would be divided into 2 pages, each of 75,000 numbers.
  EqualPagesUpTo(usize),
  /// Divide the chunk into the exactly provdided sizes.
  ///
  /// If any of the sizes are 0 or the chunk size does not equal the sum,
  /// you will get an InvalidArgument error during compression.
  ExactPageSizes(Vec<usize>),
}

/// Default: equal pages up to 1,000,000 numbers each.
impl Default for PagingSpec {
  fn default() -> Self {
    Self::EqualPagesUpTo(DEFAULT_MAX_PAGE_SIZE)
  }
}

impl PagingSpec {
  pub(crate) fn page_sizes(&self, n: usize) -> PcoResult<Vec<usize>> {
    let page_sizes = match self {
      PagingSpec::EqualPagesUpTo(max_size) => {
        let n_pages = bits::ceil_div(n, *max_size);
        let mut res = Vec::new();
        let mut start = 0;
        for i in 0..n_pages {
          let end = ((i + 1) * n) / n_pages;
          res.push(end - start);
          start = end;
        }
        res
      }
      PagingSpec::ExactPageSizes(sizes) => sizes.to_vec(),
    };

    let sizes_n: usize = page_sizes.iter().sum();
    if sizes_n != n {
      return Err(PcoError::invalid_argument(format!(
        "paging spec suggests {} numbers but {} were given",
        sizes_n, n,
      )));
    }

    for &size in &page_sizes {
      if size == 0 {
        return Err(PcoError::invalid_argument(
          "cannot write data page of 0 numbers",
        ));
      }
    }

    Ok(page_sizes)
  }
}
