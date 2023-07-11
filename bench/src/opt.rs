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
  /// Comma-separated substrings of datasets to benchmark.
  /// By default all datasets are run.
  #[arg(long, short, default_value = "", value_delimiter = ',')]
  pub datasets: Vec<String>,
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
