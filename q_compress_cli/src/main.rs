use anyhow::Result;
use structopt::StructOpt;

use crate::opt::Opt;

mod compress;
mod inspect;
mod opt;
mod unknown_compressor;
mod unknown_inspector;
mod utils;
mod dtype;
mod universal_number_like;

fn main() -> Result<()> {
  let opt = Opt::from_args();
  match opt {
    Opt::Inspect(inspect_opt) => inspect::inspect(inspect_opt)?,
    Opt::Compress(compress_opt) => compress::compress(compress_opt)?,
  }
  Ok(())
}