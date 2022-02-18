use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Clone, Debug, StructOpt)]
#[structopt {name = "Q Compress CLI", about = "A command line tool to compress, decompress, and inspect .qco files"}]
pub enum Opt {
  // #[structopt(name="compress")]
  // Compress(CompressOpt),
  // #[structopt(name="decompress")]
  // Decompress(DecompressOpt),
  #[structopt(name="inspect")]
  Inspect(InspectOpt),
}

// #[derive(Clone, Debug, StructOpt)]
// pub struct CompressOpt {
//   #[structopt(long="csv")]
//   csv: Option<PathBuf>,
//   #[structopt(short="l", long="level", default_value="6")]
//   level: usize,
//   #[structopt(short="d", long="delta-order", default_value="0")]
//   delta_encoding_order: usize,
// }
//
// #[derive(Clone, Debug, StructOpt)]
// pub struct DecompressOpt {
//
// }

#[derive(Clone, Debug, StructOpt)]
pub struct InspectOpt {
  pub path: PathBuf,
}
