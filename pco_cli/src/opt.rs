use clap::{Parser, Subcommand};
use std::path::PathBuf;

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
  #[arg(long = "csv", help = "path to decompress a column from")]
  pub csv_path: Option<PathBuf>,
  #[arg(long = "parquet", short, help = "path to decompress a column from")]
  pub parquet_path: Option<PathBuf>,
  #[arg(
    long = "binary",
    help = "path to interpret as a little-endian array of numbers"
  )]
  pub binary_path: Option<PathBuf>,

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
