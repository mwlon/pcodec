use crate::constants::{Bitlen, DEFAULT_MAX_PAGE_N};
use crate::errors::{PcoError, PcoResult};
use crate::DEFAULT_COMPRESSION_LEVEL;

/// Specifies how Pco should choose a [`mode`][crate::Mode] to compress this
/// chunk of data.
///
/// The `Try*` variants almost always use the provided mode, but fall back to an
/// effectively uncompressed version of `Classic` if the provided mode is
/// especially bad.
/// It is recommended that you only use the `Try*` variants if you know for
/// certain that your numbers benefit from that mode.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
#[non_exhaustive]
pub enum ModeSpec {
  /// Automatically detect a good mode.
  ///
  /// This works well most of the time, but costs some compression time and can
  /// select a bad mode in adversarial cases.
  #[default]
  Auto,
  /// Only use `Classic` mode.
  Classic,
  /// Try using `FloatMult` mode with a given `base`.
  ///
  /// Only applies to floating-point types.
  TryFloatMult(f64),
  /// Try using `FloatQuant` mode with `k` bits of quantization.
  ///
  /// Only applies to floating-point types.
  TryFloatQuant(Bitlen),
  /// Try using `IntMult` mode with a given `base`.
  ///
  /// Only applies to integer types.
  TryIntMult(u64),
}

// TODO consider adding a "lossiness" spec that allows dropping secondary latent
// vars.
/// All configurations available for a compressor.
///
/// Some, like `delta_encoding_order`, are explicitly stored in the
/// compressed bytes.
/// Others, like `compression_level`, affect compression but are not explicitly
/// stored.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ChunkConfig {
  /// Ranges from 0 to 12 inclusive (default: 8).
  ///
  /// At present,
  /// * Level 0 achieves only a small amount of compression.
  /// * Level 8 achieves very good compression.
  /// * Level 12 achieves marginally better compression than 8.
  ///
  /// The meaning of the compression levels is subject to change with
  /// new releases.
  pub compression_level: usize,
  /// Ranges from 0 to 7 inclusive (default: `None`, automatically detecting on
  /// each chunk).
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
  /// chunks, you can create a
  /// [`ChunkDecompressor`][crate::wrapped::ChunkDecompressor] and read the
  /// delta encoding order it chose.
  pub delta_encoding_order: Option<usize>,
  /// Specifies how the mode should be determined.
  ///
  /// See [`Mode`](crate::Mode) to understand what modes are.
  pub mode_spec: ModeSpec,
  /// Specifies how the chunk should be split into pages (default: equal pages
  /// up to 2^18 numbers each).
  pub paging_spec: PagingSpec,
}

impl Default for ChunkConfig {
  fn default() -> Self {
    Self {
      compression_level: DEFAULT_COMPRESSION_LEVEL,
      delta_encoding_order: None,
      mode_spec: ModeSpec::default(),
      paging_spec: PagingSpec::EqualPagesUpTo(DEFAULT_MAX_PAGE_N),
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

  /// Sets [`mode_spec`][ChunkConfig::mode_spec].
  pub fn with_mode_spec(mut self, mode_spec: ModeSpec) -> Self {
    self.mode_spec = mode_spec;
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
  /// Divide the chunk into equal pages of up to this many numbers.
  ///
  /// For example, with equal pages up to 100,000, a chunk of 150,000
  /// numbers would be divided into 2 pages, each of 75,000 numbers.
  EqualPagesUpTo(usize),
  /// Divide the chunk into the exactly provided counts.
  ///
  /// Will return an InvalidArgument error during compression if
  /// any of the counts are 0 or the sum does not equal the chunk count.
  Exact(Vec<usize>),
}

impl Default for PagingSpec {
  fn default() -> Self {
    Self::EqualPagesUpTo(DEFAULT_MAX_PAGE_N)
  }
}

impl PagingSpec {
  pub(crate) fn n_per_page(&self, n: usize) -> PcoResult<Vec<usize>> {
    let n_per_page = match self {
      // You might think it would be beneficial to do either of these:
      // * greedily fill pages since compressed chunk size seems like a concave
      //   function of chunk_n
      // * limit most pages to full batches for efficiency
      //
      // But in practice compressed chunk size has an inflection point upward
      // at some point, so the first idea doesn't work.
      // And the 2nd idea has only shown mixed/negative results, so I'm leaving
      // this as-is.
      PagingSpec::EqualPagesUpTo(max_page_n) => {
        let n_pages = n.div_ceil(*max_page_n);
        let mut res = Vec::new();
        let mut start = 0;
        for i in 0..n_pages {
          let end = ((i + 1) * n) / n_pages;
          res.push(end - start);
          start = end;
        }
        res
      }
      PagingSpec::Exact(n_per_page) => n_per_page.to_vec(),
    };

    let summed_n: usize = n_per_page.iter().sum();
    if summed_n != n {
      return Err(PcoError::invalid_argument(format!(
        "paging spec suggests {} numbers but {} were given",
        summed_n, n,
      )));
    }

    for &page_n in &n_per_page {
      if page_n == 0 {
        return Err(PcoError::invalid_argument(
          "cannot write data page of 0 numbers",
        ));
      }
    }

    Ok(n_per_page)
  }
}
