use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

use pco::{FloatMultSpec, FloatQuantSpec, IntMultSpec};

use crate::input::{InputColumnOpt, InputFileOpt};
use crate::{arrow_handlers, input};
use crate::{parse, utils};

pub mod handler;

/// Compress from a different format into standalone .pco
#[derive(Clone, Debug, Parser)]
pub struct CompressOpt {
  /// Compression level.
  #[arg(long, default_value = "8")]
  pub level: usize,
  /// If specified, uses a fixed delta encoding order. Defaults to automatic detection.
  #[arg(long = "delta-order")]
  pub delta_encoding_order: Option<usize>,
  /// Can be "Enabled", "Disabled", or a fixed integer to use as the base in
  /// int mult mode.
  #[arg(long, default_value = "Enabled", value_parser = parse::int_mult)]
  pub int_mult: IntMultSpec,
  /// Can be "Enabled", "Disabled", or a fixed float to use as the base in
  /// float mult mode.
  #[arg(long, default_value = "Enabled", value_parser = parse::float_mult)]
  pub float_mult: FloatMultSpec,
  /// Can be "Disabled", or a fixed integer to use as the parameter `k` in float quant mode.
  /// TODO(https://github.com/mwlon/pcodec/issues/194): Implement "Enabled" mode
  #[arg(long, default_value = "Disabled", value_parser = parse::float_quant)]
  pub float_quant: FloatQuantSpec,
  #[arg(long, default_value_t=pco::DEFAULT_MAX_PAGE_N)]
  pub chunk_size: usize,
  /// Overwrite the output path (if it exists) instead of failing.
  #[arg(long)]
  pub overwrite: bool,
  #[command(flatten)]
  pub input_file: InputFileOpt,
  #[command(flatten)]
  pub input_column: InputColumnOpt,

  /// Output .pco path to write to.
  pub path: PathBuf,
}

pub fn compress(opt: CompressOpt) -> Result<()> {
  let schema = input::get_schema(&opt.input_column, &opt.input_file)?;
  let col_idx = utils::find_col_idx(
    &schema,
    opt.input_column.col_idx,
    &opt.input_column.col_name,
  )?;
  let dtype = schema.field(col_idx).data_type();
  let handler = arrow_handlers::from_dtype(dtype)?;
  handler.compress(&opt, &schema)
}
