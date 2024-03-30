use std::path::PathBuf;
use std::str::FromStr;

use anyhow::Result;
use arrow::datatypes::DataType;
use clap::{Args, Parser};

use crate::bench::codecs::CodecConfig;
use crate::opt::InputFileOpt;
use crate::parse;

#[derive(Clone, Debug, Parser)]
pub struct BenchOpt {
  // TODO explain some way to get the list of keys available
  /// Comma-separated list of codecs to benchmark, optionally with
  /// colon-separated configurations.
  ///
  /// For example, setting this to
  /// `zstd,zstd:level=10,pco:level=9:delta_order=0`
  /// will compare 3 codecs: zstd at default compression level (3), zstd at
  /// level 10, and pco at level 9 with 0th order delta encoding.
  #[arg(long, short, default_value = "pco", value_parser = CodecConfig::from_str, value_delimiter = ',')]
  pub codecs: Vec<CodecConfig>,
  /// Comma-separated substrings of datasets or column names to benchmark.
  /// By default all datasets are run.
  #[arg(long, short, default_values_t = Vec::<String>::new(), value_delimiter = ',')]
  pub datasets: Vec<String>,
  /// Path to a parquet file to use as input.
  /// Only numerical columns, non-null values in the file will be used.
  #[arg(long = "parquet", short)]
  pub parquet_path: Option<PathBuf>,
  /// Path to a CSV file to use as input.
  /// Only numerical columns, non-null values in the file will be used.
  #[arg(long = "csv")]
  pub csv_path: Option<PathBuf>,
  /// Filter down to datasets or columns matching this Arrow data type,
  /// e.g. i32 or micros.
  #[arg(long, default_values_t = Vec::<String>::new(), value_parser = parse::arrow_dtype, value_delimiter = ',')]
  pub dtypes: Vec<DataType>,
  /// Number of iterations to run each codec x dataset combination for
  /// better estimation of durations.
  /// The median duration is kept.
  #[arg(long, short, default_value = "10")]
  pub iters: usize,
  /// How many numbers to limit each dataset to.
  #[arg(long, short)]
  pub limit: Option<usize>,
  #[command(flatten)]
  pub input: InputFileOpt,
  #[command(flatten)]
  pub handler_opt: HandlerOpt,
}

#[derive(Clone, Debug, Args)]
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

impl BenchOpt {
  pub fn includes_dtype_str(&self, dtype_str: &str) -> Result<bool> {
    let dtype = parse::arrow_dtype(dtype_str)?;
    if self.dtypes.is_empty() {
      Ok(true)
    } else {
      Ok(self.dtypes.contains(&dtype))
    }
  }

  pub fn includes_dataset(&self, dataset: &str) -> bool {
    self.datasets.is_empty()
      || self
        .datasets
        .iter()
        .any(|allowed_substr| dataset.contains(allowed_substr))
  }
}
