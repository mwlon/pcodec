use anyhow::Result;
use clap::Parser;

use crate::opt::{Opt, OptWrapper};

mod arrow_handlers;
mod compress;
mod compress_handler;
mod core_handlers;
mod decompress;
mod decompress_handler;
mod dtypes;
mod inspect;
mod inspect_handler;
mod opt;
mod utils;

fn main() -> Result<()> {
  let opt = OptWrapper::parse().opt;
  match opt {
    Opt::Compress(compress_opt) => compress::compress(compress_opt)?,
    Opt::Decompress(decompress_opt) => decompress::decompress(decompress_opt)?,
    Opt::Inspect(inspect_opt) => inspect::inspect(inspect_opt)?,
  }
  Ok(())
}
