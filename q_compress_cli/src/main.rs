use anyhow::Result;
use structopt::StructOpt;

use crate::opt::Opt;

mod opt;
mod inspect;
mod utils;
mod unknown_decompressor;


fn main() -> Result<()> {
  let opt = Opt::from_args();
  match opt {
    Opt::Inspect(inspect_opt) => inspect::inspect(inspect_opt)?,
  }
  Ok(())
}