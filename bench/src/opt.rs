use std::path::PathBuf;
use std::str::FromStr;

use clap::{Args, Parser};

use crate::codecs::CodecConfig;

#[derive(Parser)]
pub struct Opt {
  /// Comma-separated list of codecs to benchmark, optionally with
  /// colon-separated configurations.
  ///
  /// For example, setting this to
  /// `zstd,zstd:level=10,pco:level=9:delta_order=0`
  /// will compare 3 codecs: zstd at default compression level (3), zstd at
  /// level 10, and pco at level 9 with 0th order delta encoding.
  /// See the code in src/codecs/*.rs for configurations available to each
  /// codec.
  #[arg(long, short, default_value = "pco", value_parser=CodecConfig::from_str, value_delimiter=',')]
  pub codecs: Vec<CodecConfig>,
  /// Comma-separated substrings of synthetic datasets to benchmark.
  /// By default all synthetic datasets are run.
  #[arg(long, short, default_values_t = Vec::<String>::new(), value_delimiter = ',')]
  pub datasets: Vec<String>,
  /// Path to a parquet file to use as input.
  /// Only numerical columns in the file will be used.
  /// Only non-null values will be used.
  #[arg(long, short)]
  pub parquet_dataset: Option<PathBuf>,
  /// Filter down to datasets or columns matching this data type,
  /// e.g. i32.
  #[arg(long, default_values_t = Vec::<String>::new(), value_delimiter = ',')]
  pub dtypes: Vec<String>,
  /// Number of iterations to run each codec x dataset combination for
  /// better estimation of durations.
  /// The median duration is kept.
  #[arg(long, short, default_value = "10")]
  pub iters: usize,
  #[command(flatten)]
  pub handler_opt: HandlerOpt,
}

#[derive(Args)]
pub struct HandlerOpt {
  #[arg(long)]
  pub no_compress: bool,
  #[arg(long)]
  pub no_decompress: bool,
  /// Skip assertions that all the numbers came back bitwise identical.
  ///
  /// This does not affect benchmark timing.
  #[arg(long)]
  pub no_assertions: bool,
}

impl Opt {
  pub fn includes_dtype(&self, dtype: &str) -> bool {
    self.dtypes.is_empty()
      || self
        .dtypes
        .iter()
        .any(|allowed_dtype| allowed_dtype == dtype)
  }

  pub fn includes_dataset(&self, dataset: &str) -> bool {
    self.datasets.is_empty()
      || self
        .datasets
        .iter()
        .any(|allowed_substr| dataset.contains(allowed_substr))
  }
}
