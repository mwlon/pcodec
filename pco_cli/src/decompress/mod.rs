use std::fs::OpenOptions;
use std::io::{ErrorKind, Read};

use anyhow::Result;

use crate::opt::DecompressOpt;
use crate::{core_handlers, utils};

pub mod decompress_handler;

pub fn decompress(opt: DecompressOpt) -> Result<()> {
  let mut initial_bytes = vec![0; pco::standalone::guarantee::header_size() + 1];
  match OpenOptions::new()
    .read(true)
    .open(&opt.pco_path)?
    .read_exact(&mut initial_bytes)
  {
    Ok(()) => (),
    Err(e) if matches!(e.kind(), ErrorKind::UnexpectedEof) => (),
    other => other?,
  };
  let Some(dtype) = utils::get_standalone_dtype(&initial_bytes)? else {
    // file terminated; nothing to decompress
    return Ok(());
  };
  let handler = core_handlers::from_dtype(dtype);
  handler.decompress(&opt)
}
