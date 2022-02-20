use std::path::PathBuf;
use structopt::StructOpt;
use crate::dtype::DType;
use anyhow::anyhow;
use anyhow::Result;

#[derive(Clone, Debug, StructOpt)]
#[structopt {
  name = "q_compress CLI",
  about = "A command line tool to compress, decompress, and inspect .qco files",
}]
pub enum Opt {
  #[structopt(name="compress")]
  Compress(CompressOpt),
  // #[structopt(name="decompress")]
  // Decompress(DecompressOpt),
  #[structopt(name="inspect")]
  Inspect(InspectOpt),
}

#[derive(Clone, Debug, StructOpt)]
pub struct CompressOpt {
  #[structopt(long="csv")]
  pub csv_path: Option<PathBuf>,
  #[structopt(long="parquet")]
  pub parquet_path: Option<PathBuf>,

  #[structopt(short="l", long="level", default_value="6")]
  pub level: usize,
  #[structopt(long="delta-order", default_value="0")]
  pub delta_encoding_order: usize,
  #[structopt(long="dtype")]
  pub dtype: Option<DType>,
  #[structopt(long="col-name")]
  pub col_name: Option<String>,
  #[structopt(long="col-idx")]
  pub col_idx: Option<usize>,
  #[structopt(long="chunk-size", default_value="1000000")]
  pub chunk_size: usize,
  #[structopt(long="overwrite")]
  pub overwrite: bool,
  #[structopt(long="csv-has-header")]
  pub has_csv_header: bool,
  #[structopt(long="csv-timestamp-format", default_value="%Y-%m-%dT%H:%M:%s%.f%z")]
  pub timestamp_format: String,
  #[structopt(long="csv-delimiter", default_value=",")]
  pub delimiter: char,

  pub qco_path: PathBuf,
}

impl CompressOpt {
  pub fn csv_has_header(&self) -> Result<bool> {
    let res = match (&self.col_name, &self.col_idx) {
      (Some(_), None) => Ok(true),
      (None, Some(_)) => Ok(self.has_csv_header),
      _ => Err(anyhow!("conflicting or incomplete CSV column information")),
    }?;

    Ok(res)
  }
}

#[derive(Clone, Debug, StructOpt)]
pub struct InspectOpt {
  pub path: PathBuf,
}
