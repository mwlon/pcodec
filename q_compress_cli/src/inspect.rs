use std::fs;

use anyhow::Result;

use crate::opt::InspectOpt;
use crate::unknown_inspector;
use crate::utils;

pub fn inspect(opt: InspectOpt) -> Result<()> {
  let bytes = fs::read(&opt.path)?;
  let header_byte = utils::get_header_byte(&bytes)?;
  let decompressor = unknown_inspector::new(header_byte)?;
  decompressor.inspect(&bytes, opt)
}