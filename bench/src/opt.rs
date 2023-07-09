use clap::Parser;
use crate::codec_config::CodecConfig;

pub const AUTO_DELTA: usize = usize::MAX;

#[derive(Parser)]
pub struct Opt {
  #[arg(long, short, default_value = "pco", value_parser=parse_codec, value_delimiter=',')]
  pub codecs: Vec<CodecConfig>,
  #[arg(long, short, default_value = "", value_delimiter=',')]
  pub datasets: Vec<String>,
  #[arg(long, short, default_value = "10")]
  pub iters: usize,
  #[arg(long)]
  pub no_compress: bool,
  #[arg(long)]
  pub no_decompress: bool,
  #[arg(long)]
  pub no_assertions: bool,
}

pub fn parse_codec(s: &str) -> Result<CodecConfig, &'static str> {
  let parts = s.split(':').collect::<Vec<_>>();
  let level = if parts.len() > 1 {
    Some(parts[1].parse().unwrap())
  } else {
    None
  };
  match parts[0] {
    "p" | "pco" | "pcodec" => {
      let delta_encoding_order = if parts.len() > 2 {
        parts[2].parse().unwrap()
      } else {
        AUTO_DELTA
      };
      let use_gcds = !(parts.len() > 3 && &parts[3].to_lowercase()[0..3] == "off");
      let config = pco::CompressorConfig::default()
        .with_compression_level(level.unwrap_or(q_compress::DEFAULT_COMPRESSION_LEVEL))
        .with_delta_encoding_order(delta_encoding_order)
        .with_use_gcds(use_gcds);
      Ok(CodecConfig::Pco(config))
    }
    "q" | "qco" | "q_compress" => {
      let delta_encoding_order = if parts.len() > 2 {
        parts[2].parse().unwrap()
      } else {
        AUTO_DELTA
      };
      let use_gcds = !(parts.len() > 3 && &parts[3].to_lowercase()[0..3] == "off");
      let config = q_compress::CompressorConfig::default()
        .with_compression_level(level.unwrap_or(q_compress::DEFAULT_COMPRESSION_LEVEL))
        .with_delta_encoding_order(delta_encoding_order)
        .with_use_gcds(use_gcds);
      Ok(CodecConfig::QCompress(config))
    }
    "zstd" => Ok(CodecConfig::ZStd(level.unwrap_or(3))),
    _ => Err("unknown compressor"),
  }
}

