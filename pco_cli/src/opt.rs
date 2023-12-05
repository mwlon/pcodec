use std::path::PathBuf;

use anyhow::anyhow;
use anyhow::Result;
use clap::{Parser, Subcommand};

use crate::dtype::DType;

#[derive(Clone, Debug, Parser)]
#[command(about = "compress, decompress, and inspect .pco files")]
pub struct OptWrapper {
  #[command(subcommand)]
  pub opt: Opt,
}

#[derive(Subcommand, Clone, Debug)]
pub enum Opt {
  Compress(CompressOpt),
  Decompress(DecompressOpt),
  Inspect(InspectOpt),
}

#[derive(Clone, Debug, Parser)]
#[command(about = "compress from a different format into standalone .pco")]
pub struct CompressOpt {
  #[arg(long = "csv")]
  pub csv_path: Option<PathBuf>,
  #[arg(long = "parquet")]
  pub parquet_path: Option<PathBuf>,

  #[arg(long, default_value = "8")]
  pub level: usize,
  #[arg(long = "delta-order")]
  pub delta_encoding_order: Option<usize>,
  #[arg(long)]
  pub disable_int_mult: bool,
  #[arg(long)]
  pub dtype: Option<DType>,
  #[arg(long)]
  pub col_name: Option<String>,
  #[arg(long)]
  pub col_idx: Option<usize>,
  #[arg(long, default_value = "262144")]
  pub chunk_size: usize,
  #[arg(long)]
  pub overwrite: bool,
  #[arg(long = "csv-has-header")]
  pub has_csv_header: bool,
  #[arg(
    long = "csv-timestamp-format",
    default_value = "%Y-%m-%dT%H:%M:%S%.f%z"
  )]
  pub timestamp_format: String,
  #[arg(long = "csv-delimiter", default_value = ",")]
  pub delimiter: char,

  pub pco_path: PathBuf,
}

impl CompressOpt {
  pub fn csv_has_header(&self) -> Result<bool> {
    let res = match (&self.col_name, &self.col_idx) {
      (Some(_), None) => Ok(true),
      (None, Some(_)) => Ok(self.has_csv_header),
      _ => Err(anyhow!(
        "conflicting or incomplete CSV column information"
      )),
    }?;

    Ok(res)
  }
}

#[derive(Clone, Debug, Parser)]
#[command(about = "decompress from standalone .pco into stdout")]
pub struct DecompressOpt {
  #[arg(long)]
  pub limit: Option<usize>,
  // TODO either make this do something or remove it
  #[arg(long, default_value = "%Y-%m-%dT%H:%M:%S%.f")]
  pub timestamp_format: String,

  pub pco_path: PathBuf,
}

#[derive(Clone, Debug, Parser)]
#[command(about = "print metadata about a standalone .pco file")]
pub struct InspectOpt {
  pub path: PathBuf,
}
