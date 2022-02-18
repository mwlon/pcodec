use std::fs;
use crate::unknown_decompressor;
use crate::utils;
use anyhow::Result;

use crate::opt::InspectOpt;

pub fn inspect(opt: InspectOpt) -> Result<()> {
  let bytes = fs::read(&opt.path)?;
  let header_byte = utils::get_header_byte(&bytes)?;
  let decompressor = unknown_decompressor::new(header_byte)?;
  decompressor.inspect(&bytes, opt)
}