use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::bench::BenchOpt;
use crate::compress::CompressOpt;
use crate::decompress::DecompressOpt;
use crate::inspect::InspectOpt;

#[derive(Clone, Debug, Parser)]
#[command(about = "compress, decompress, and inspect .pco files")]
pub struct OptWrapper {
  #[command(subcommand)]
  pub opt: Opt,
}

#[derive(Clone, Debug, Parser)]
pub struct InputFileOpt {
  /// Path to a Parquet file to use as input.
  /// Only numerical, non-null values in the file will be used.
  #[arg(long = "parquet", short)]
  pub parquet_path: Option<PathBuf>,
  /// Path to a CSV file to use as input.
  /// Only numerical, non-null values in the file will be used.
  #[arg(long = "csv")]
  pub csv_path: Option<PathBuf>,

  #[arg(long)]
  pub csv_has_header: bool,
  #[arg(long, default_value = "%Y-%m-%dT%H:%M:%S%.f%z")]
  pub csv_timestamp_format: String,
  #[arg(long, default_value = ",")]
  pub csv_delimiter: char,
}

#[derive(Subcommand, Clone, Debug)]
pub enum Opt {
  Bench(BenchOpt),
  Compress(CompressOpt),
  Decompress(DecompressOpt),
  Inspect(InspectOpt),
}
