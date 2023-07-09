use std::str::FromStr;

use clap::{Args, Parser};

use crate::codecs::CodecConfig;

#[derive(Parser)]
pub struct Opt {
  #[arg(long, short, default_value = "pco", value_parser=CodecConfig::from_str, value_delimiter=',')]
  pub codecs: Vec<CodecConfig>,
  #[arg(long, short, default_value = "", value_delimiter=',')]
  pub datasets: Vec<String>,
  #[arg(long, short, default_value = "10")]
  pub iters: usize,
  #[command(flatten)]
  pub handler_opt: HandlerOpt,
}

#[derive(Args)]
pub struct HandlerOpt {
  #[arg(long)]
  pub no_compress: bool,
  #[arg(long)]
  pub no_decompress: bool,
  #[arg(long)]
  pub no_assertions: bool,
}
