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

#[derive(Subcommand, Clone, Debug)]
pub enum Opt {
  Bench(BenchOpt),
  Compress(CompressOpt),
  Decompress(DecompressOpt),
  Inspect(InspectOpt),
}
