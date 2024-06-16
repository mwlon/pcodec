use clap::Parser;

use pco::{ChunkConfig, FloatMultSpec, FloatQuantSpec, IntMultSpec, PagingSpec};

use crate::parse;

pub const AUTO_DELTA_ORDER: usize = usize::MAX;

#[derive(Clone, Debug, Parser)]
pub struct ChunkConfigOpt {
  /// Compression level.
  #[arg(long, default_value = "8")]
  pub level: usize,
  /// Can be an integer for how many times to apply delta encoding, or "Auto",
  /// which tries to automatically detect the best delta encoding order.
  #[arg(long = "delta-order", default_value = "Auto", value_parser = parse::delta_encoding_order)]
  pub delta_encoding_order: usize,
  /// Can be "Enabled", "Disabled", or a fixed integer to use as the base in
  /// int mult mode.
  #[arg(long, default_value = "Enabled", value_parser = parse::int_mult)]
  pub int_mult: IntMultSpec,
  /// Can be "Enabled", "Disabled", or a fixed float to use as the base in
  /// float mult mode.
  #[arg(long, default_value = "Enabled", value_parser = parse::float_mult)]
  pub float_mult: FloatMultSpec,
  // TODO(https://github.com/mwlon/pcodec/issues/194): Implement "Enabled" mode
  /// Can be "Disabled", or a fixed integer to use as the parameter `k` in float quant mode.
  #[arg(long, default_value = "Disabled", value_parser = parse::float_quant)]
  pub float_quant: FloatQuantSpec,
  #[arg(long, default_value_t = pco::DEFAULT_MAX_PAGE_N)]
  pub chunk_n: usize,
}

impl ChunkConfigOpt {
  pub fn delta_encoding_order(&self) -> Option<usize> {
    if self.delta_encoding_order == AUTO_DELTA_ORDER {
      None
    } else {
      Some(self.delta_encoding_order)
    }
  }
}

impl From<&ChunkConfigOpt> for ChunkConfig {
  fn from(opt: &ChunkConfigOpt) -> Self {
    ChunkConfig::default()
      .with_compression_level(opt.level)
      .with_delta_encoding_order(opt.delta_encoding_order())
      .with_int_mult_spec(opt.int_mult)
      .with_float_mult_spec(opt.float_mult)
      .with_float_quant_spec(opt.float_quant)
      .with_paging_spec(PagingSpec::EqualPagesUpTo(opt.chunk_n))
  }
}
