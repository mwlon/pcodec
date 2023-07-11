use anyhow::Result;
use clap::Parser;

use crate::opt::{Opt, OptWrapper};

mod compress;
mod compress_handler;
mod decompress;
mod decompress_handler;
mod dtype;
mod handlers;
mod inspect;
mod inspect_handler;
mod number_like_arrow;
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
