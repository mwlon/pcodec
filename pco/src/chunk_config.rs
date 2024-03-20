use crate::constants::DEFAULT_MAX_PAGE_N;
use crate::errors::{PcoError, PcoResult};
use crate::{DEFAULT_COMPRESSION_LEVEL, FULL_BATCH_N};
use std::cmp::{max, min};

/// Configures whether integer multiplier detection is enabled.
///
/// Examples where this helps:
/// * nanosecond-precision timestamps that are mostly whole numbers of
/// microseconds, with a few exceptions
/// * integers `[7, 107, 207, 307, ... 100007]` shuffled
///
/// When this is helpful, compression and decompression speeds can be
/// substantially reduced. This configuration may hurt
/// compression speed slightly even when it isn't helpful.
/// However, the compression ratio improvements tend to be large.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum IntMultSpec {
  Disabled,
  #[default]
  Enabled,
  /// If you know all your ints are roughly multiples of `base` (or have a
  /// simple distribution modulo `base`), you can provide `base` here to
  /// ensure it gets used and save compression time.
  Provided(u64),
}

/// Configures whether float multiplier detection is enabled.
///
/// Examples where this helps:
/// * approximate multiples of 0.01
/// * approximate multiples of pi
///
/// Float mults can work even when there are NaNs and infinities.
/// When this is helpful, compression and decompression speeds can be
/// substantially reduced. In rare cases, this configuration
/// may reduce compression speed somewhat even when it isn't helpful.
/// However, the compression ratio improvements tend to be large.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub enum FloatMultSpec {
  Disabled,
  #[default]
  Enabled,
  /// If you know all your floats are roughly multiples of `base`, you can
  /// provide `base` here to ensure it gets used and save compression time.
  Provided(f64),
  // TODO support a LossyEnabled mode that always drops the ULPs latent var
}

/// All configurations available for a compressor.
///
/// Some, like `delta_encoding_order`, are explicitly stored in the
/// compressed bytes.
/// Others, like `compression_level`, affect compression but are not explicitly
/// stored in the output.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ChunkConfig {
  /// `compression_level` ranges from 0 to 12 inclusive (default: 8).
  ///
  /// At present,
  /// * Level 0 achieves only a small amount of compression.
  /// * Level 8 achieves very good compression and runs
  /// only slightly slower.
  /// * Level 12 achieves marginally better compression than 8
  /// and may run several times slower.
  ///
  /// At present, the compression levels cover a relatively small range of the
  /// compression time vs. ratio tradeoff.
  /// However, the meaning of the compression levels is subject to change with
  /// new releases.
  pub compression_level: usize,
  /// `delta_encoding_order` ranges from 0 to 7 inclusive (default:
  /// `None`, automatically detecting on each chunk).
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
  /// [`auto_delta_encoding_order`][crate::auto_delta_encoding_order] can help.
  pub delta_encoding_order: Option<usize>,
  /// Integer multiplier mode improves compression ratio in cases where many
  /// numbers are congruent modulo an integer `base`
  /// (default: `Enabled`).
  ///
  /// See [`IntMultSpec`][crate::IntMultSpec] for more detail.
  pub int_mult_spec: IntMultSpec,
  /// Float multiplier mode improves compression ratio in cases where the data
  /// type is a float and all numbers are close to a multiple of a float
  /// `base`
  /// (default: `Enabled`).
  ///
  /// See [`FloatMultSpec`][crate::FloatMultSpec] for more detail.
  pub float_mult_spec: FloatMultSpec,
  /// `paging_spec` specifies how the chunk should be split into pages
  /// (default: equal pages up to 2^18 numbers each).
  ///
  /// See [`PagingSpec`][crate::PagingSpec] for more information.
  pub paging_spec: PagingSpec,
}

impl Default for ChunkConfig {
  fn default() -> Self {
    Self {
      compression_level: DEFAULT_COMPRESSION_LEVEL,
      delta_encoding_order: None,
      int_mult_spec: IntMultSpec::Enabled,
      float_mult_spec: FloatMultSpec::Enabled,
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

  /// Sets [`int_mult_spec`][ChunkConfig::int_mult_spec].
  pub fn with_int_mult_spec(mut self, int_mult_spec: IntMultSpec) -> Self {
    self.int_mult_spec = int_mult_spec;
    self
  }

  /// Sets [`float_mult_spec`][ChunkConfig::float_mult_spec].
  pub fn with_float_mult_spec(mut self, float_mult_spec: FloatMultSpec) -> Self {
    self.float_mult_spec = float_mult_spec;
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
  /// Divide the chunk by whole batches into roughly equal-sized pages of up to
  /// this many numbers.
  ///
  /// The limit must be a multiple of 256 (pco's batch size) or else an error
  /// will be caused during compression.
  /// Only the last page may be jagged.
  /// For example, with equal pages up to 4,096, a chunk of 5,000
  /// numbers would be divided into 2 pages: one with 2,560 and another with
  /// 2,440.
  EqualPagesUpTo(usize),
  /// Divide the chunk into the exactly provided counts.
  ///
  /// Will cause an InvalidArgument error during compression if
  /// any of the counts are 0 or the sum does not equal the count of numbers
  /// in the chunk.
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
      &PagingSpec::EqualPagesUpTo(max_page_n) => {
        if max_page_n % 256 != 0 {
          return Err(PcoError::invalid_argument(format!(
            "page size limit must be a multiple of {} when paging by equal pages (was {})",
            FULL_BATCH_N, max_page_n,
          )));
        }

        let n_pages = n.div_ceil(max_page_n);
        let n_batches = n / FULL_BATCH_N;
        let min_n_per_page = FULL_BATCH_N * (n_batches / max(n_pages, 1));
        let mut undershoot = n - n_pages * min_n_per_page;
        let mut res = vec![min_n_per_page; n_pages];
        let mut i = 0;
        while undershoot > 0 {
          let increment = min(FULL_BATCH_N, undershoot);
          res[i] += increment;
          undershoot -= increment;
          i += 1;
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

#[cfg(test)]
mod tests {
  use crate::errors::PcoResult;
  use crate::PagingSpec;

  fn equal_page_sizes(n: usize, max_page_n: usize) -> PcoResult<Vec<usize>> {
    PagingSpec::EqualPagesUpTo(max_page_n).n_per_page(n)
  }
  #[test]
  fn test_equal_pages_up_to() -> PcoResult<()> {
    assert_eq!(equal_page_sizes(0, 512)?, vec![]);
    assert_eq!(equal_page_sizes(1, 512)?, vec![1]);
    assert_eq!(equal_page_sizes(512, 512)?, vec![512]);
    assert_eq!(equal_page_sizes(513, 512)?, vec![257, 256]);
    assert_eq!(
      equal_page_sizes(1025, 512)?,
      vec![512, 257, 256]
    );
    assert_eq!(
      equal_page_sizes(2048, 512)?,
      vec![512, 512, 512, 512]
    );
    assert_eq!(
      equal_page_sizes(2100, 512)?,
      vec![512, 512, 512, 308, 256]
    );
    Ok(())
  }
}
