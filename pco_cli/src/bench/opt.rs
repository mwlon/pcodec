use std::path::PathBuf;
use std::str::FromStr;

use arrow::datatypes::DataType;
use clap::{Args, Parser};

use crate::bench::codecs::CodecConfig;
use crate::opt::InputFileOpt;
use crate::{dtypes, parse};

/// Run benchmarks on datasets originating from another format.
/// The input format does not affect performance; all input numbers are
/// loaded into memory prior to benchmarking each dataset.
/// This supports output formats other than pco, if compiled with the
/// necessary features.
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
  /// Filter down to datasets or columns matching this Arrow data type,
  /// e.g. i32 or micros.
  #[arg(long, default_values_t = Vec::<DataType>::new(), value_parser = parse::arrow_dtype, value_delimiter = ',')]
  pub dtypes: Vec<DataType>,
  /// Number of iterations to run each codec x dataset combination for
  /// better estimation of durations.
  /// The median duration is kept.
  #[arg(long, short, default_value = "10")]
  pub iters: usize,
  /// How many numbers to limit each dataset to.
  #[arg(long, short)]
  pub limit: Option<usize>,
  /// Path to a directory containing binary files to be used as input.
  /// Each binary file must be prefixed with its data type, e.g.
  /// `f32_foo.bar`.
  /// By default, if no inputs are specified, the benchmarks will use the
  /// relative directory `data/binary/` as input.
  #[arg(long)]
  pub binary_dir: Option<PathBuf>,
  #[command(flatten)]
  pub input: InputFileOpt,
  #[command(flatten)]
  pub iter_opt: IterOpt,
}

#[derive(Clone, Debug, Args)]
pub struct IterOpt {
  #[arg(long)]
  pub no_compress: bool,
  #[arg(long)]
  pub no_decompress: bool,
  /// Skip assertions that all the numbers came back bitwise identical.
  ///
  /// This does not affect benchmark timing.
  #[arg(long)]
  pub no_assertions: bool,
  /// Optionally, a directory to save the compressed data to.
  /// Will overwrite conflicting files.
  #[arg(long)]
  pub save_dir: Option<PathBuf>,
}

impl BenchOpt {
  pub fn includes_dataset(&self, dtype: &DataType, name: &str) -> bool {
    if matches!(dtypes::from_arrow(dtype), Err(_))
      || (!self.dtypes.is_empty() && !self.dtypes.contains(dtype))
    {
      return false;
    }

    self.datasets.is_empty()
      || self
        .datasets
        .iter()
        .any(|allowed_substr| name.contains(allowed_substr))
  }
}
