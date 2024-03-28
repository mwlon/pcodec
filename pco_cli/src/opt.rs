use std::path::PathBuf;

use anyhow::anyhow;
use anyhow::Result;
use clap::{Parser, Subcommand};

use pco::{FloatMultSpec, IntMultSpec};
use pco::data_types::CoreDataType;

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

fn parse_int_mult(s: &str) -> Result<IntMultSpec> {
  let lowercase = s.to_lowercase();
  let spec = match lowercase.as_str() {
    "enabled" => IntMultSpec::Enabled,
    "disabled" => IntMultSpec::Disabled,
    other => match other.parse::<u64>() {
      Ok(mult) => IntMultSpec::Provided(mult),
      _ => return Err(anyhow!("cannot parse int mult: {}", other)),
    },
  };
  Ok(spec)
}

fn parse_float_mult(s: &str) -> Result<FloatMultSpec> {
  let lowercase = s.to_lowercase();
  let spec = match lowercase.as_str() {
    "enabled" => FloatMultSpec::Enabled,
    "disabled" => FloatMultSpec::Disabled,
    other => match other.parse::<f64>() {
      Ok(mult) => FloatMultSpec::Provided(mult),
      _ => return Err(anyhow!("cannot parse float mult: {}", other)),
    },
  };
  Ok(spec)
}

fn parse_dtype(s: &str) -> Result<CoreDataType> {
  let res = match s.to_lowercase().as_str() {
    "f32" => CoreDataType::F32,
    "f64" => CoreDataType::F32,
    "i32" => CoreDataType::I32,
    "i64" => CoreDataType::I64,
    "u32" => CoreDataType::U32,
    "u64" => CoreDataType::U64,
    other => return Err(anyhow!("invalid core data type: {}", other)),
  };
  Ok(res)
}

#[derive(Clone, Debug, Parser)]
#[command(about = "compress from a different format into standalone .pco")]
pub struct CompressOpt {
  #[arg(long = "csv", help = "path to decompress a column from")]
  pub csv_path: Option<PathBuf>,
  #[arg(long = "parquet", help = "path to decompress a column from")]
  pub parquet_path: Option<PathBuf>,
  #[arg(
    long = "binary",
    help = "path to interpret as a little-endian array of numbers"
  )]
  pub binary_path: Option<PathBuf>,

  #[arg(long, default_value = "8")]
  pub level: usize,
  #[arg(long = "delta-order")]
  pub delta_encoding_order: Option<usize>,
  #[arg(long, default_value = "Enabled", value_parser = parse_int_mult)]
  pub int_mult: IntMultSpec,
  #[arg(long, default_value = "Enabled", value_parser = parse_float_mult)]
  pub float_mult: FloatMultSpec,
  #[arg(long, value_parser = parse_dtype)]
  pub dtype: Option<CoreDataType>,
  #[arg(long)]
  pub col_name: Option<String>,
  #[arg(long)]
  pub col_idx: Option<usize>,
  #[arg(long, default_value_t=pco::DEFAULT_MAX_PAGE_N)]
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
      (None, None) => Err(anyhow!(
        "must provide either --col-idx or --col-name",
      )),
      _ => Err(anyhow!(
        "cannot provide both --col-idx and --col-name"
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

  pub pco_path: PathBuf,
}

#[derive(Clone, Debug, Parser)]
#[command(about = "print metadata about a standalone .pco file")]
pub struct InspectOpt {
  pub path: PathBuf,
}
