use std::fs;

use anyhow::Result;

use crate::opt::DecompressOpt;
use crate::{handlers, utils};

pub fn decompress(opt: DecompressOpt) -> Result<()> {
  let bytes = fs::read(&opt.pco_path)?;
  let header_byte = utils::get_header_byte(&bytes)?;
  let handler = handlers::from_header_byte(header_byte)?;
  handler.decompress(&opt, &bytes)
}
