use std::path::PathBuf;

use anyhow::anyhow;
use anyhow::Result;
use structopt::StructOpt;

use crate::dtype::DType;

#[derive(Clone, Debug, StructOpt)]
#[structopt {
  name = "q_compress CLI",
  about = "A command line tool to compress, decompress, and inspect .qco files",
}]
pub enum Opt {
  #[structopt(name = "compress")]
  Compress(CompressOpt),
  #[structopt(name = "decompress")]
  Decompress(DecompressOpt),
  #[structopt(name = "inspect")]
  Inspect(InspectOpt),
}

#[derive(Clone, Debug, StructOpt)]
pub struct CompressOpt {
  #[structopt(long = "csv")]
  pub csv_path: Option<PathBuf>,
  #[structopt(long = "parquet")]
  pub parquet_path: Option<PathBuf>,

  #[structopt(long, default_value = "8")]
  pub level: usize,
  #[structopt(long = "delta-order")]
  pub delta_encoding_order: Option<usize>,
  #[structopt(long)]
  pub disable_gcds: bool,
  #[structopt(long)]
  pub dtype: Option<DType>,
  #[structopt(long)]
  pub col_name: Option<String>,
  #[structopt(long)]
  pub col_idx: Option<usize>,
  #[structopt(long, default_value = "1000000")]
  pub chunk_size: usize,
  #[structopt(long)]
  pub overwrite: bool,
  #[structopt(long = "csv-has-header")]
  pub has_csv_header: bool,
  #[structopt(
    long = "csv-timestamp-format",
    default_value = "%Y-%m-%dT%H:%M:%S%.f%z"
  )]
  pub timestamp_format: String,
  #[structopt(long = "csv-delimiter", default_value = ",")]
  pub delimiter: char,

  pub qco_path: PathBuf,
}

#[derive(Clone, Debug, StructOpt)]
pub struct DecompressOpt {
  #[structopt(long)]
  pub limit: Option<usize>,
  #[structopt(long, default_value = "%Y-%m-%dT%H:%M:%S%.f")]
  pub timestamp_format: String,

  pub qco_path: PathBuf,
}

#[derive(Clone, Debug, StructOpt)]
pub struct InspectOpt {
  pub path: PathBuf,
}
