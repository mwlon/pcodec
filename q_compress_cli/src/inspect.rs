use std::fs;

use anyhow::Result;

use crate::opt::InspectOpt;
use crate::utils;
use crate::handlers;

pub fn inspect(opt: InspectOpt) -> Result<()> {
  let bytes = fs::read(&opt.path)?;
  let header_byte = utils::get_header_byte(&bytes)?;
  let handler = handlers::from_header_byte(header_byte)?;
  handler.inspect(&opt, &bytes)
}