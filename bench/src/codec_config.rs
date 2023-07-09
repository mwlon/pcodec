use std::fmt::{Display, Formatter};

#[derive(Clone, Debug)]
pub enum CodecConfig {
  Pco(pco::CompressorConfig),
  QCompress(q_compress::CompressorConfig),
  ZStd(usize),
}

impl CodecConfig {
  pub fn codec(&self) -> &'static str {
    match self {
      CodecConfig::Pco(_) => "pco",
      CodecConfig::QCompress(_) => "qco",
      CodecConfig::ZStd(_) => "zstd",
    }
  }

  pub fn details(&self) -> String {
    match self {
      CodecConfig::Pco(config) => {
        format!(
          "{}:{}:{}",
          config.compression_level, config.delta_encoding_order, config.use_gcds
        )
      }
      CodecConfig::QCompress(config) => {
        format!(
          "{}:{}:{}",
          config.compression_level, config.delta_encoding_order, config.use_gcds
        )
      }
      CodecConfig::ZStd(level) => {
        format!("{}", level)
      }
    }
  }
}

impl Display for CodecConfig {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}:{}", self.codec(), self.details())
  }
}

