use std::fs;

use anyhow::Result;

use crate::{core_handlers, utils};
use crate::opt::DecompressOpt;

pub fn decompress(opt: DecompressOpt) -> Result<()> {
  let bytes = fs::read(&opt.pco_path)?;
  let Some(dtype) = utils::get_standalone_dtype(&bytes)? else {
    // file terminated; nothing to decompress
    return Ok(());
  };
  let handler = core_handlers::from_dtype(dtype);
  handler.decompress(&opt, &bytes)
}
