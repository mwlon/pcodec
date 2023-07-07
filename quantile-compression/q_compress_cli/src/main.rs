use anyhow::Result;
use structopt::StructOpt;

use crate::opt::Opt;

mod arrow_number_like;
mod compress;
mod compress_handler;
mod decompress;
mod decompress_handler;
mod dtype;
mod handlers;
mod inspect;
mod inspect_handler;
mod opt;
mod utils;

fn main() -> Result<()> {
  let opt = Opt::from_args();
  match opt {
    Opt::Compress(compress_opt) => compress::compress(compress_opt)?,
    Opt::Decompress(decompress_opt) => decompress::decompress(decompress_opt)?,
    Opt::Inspect(inspect_opt) => inspect::inspect(inspect_opt)?,
  }
  Ok(())
}
