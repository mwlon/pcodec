use anyhow::{anyhow, Result};

use crate::bench::codecs::CodecInternal;
use crate::dtypes::PcoNumberLike;
use clap::Parser;

#[derive(Clone, Debug, Default, Parser)]
pub struct QcoConfig {
  use_fixed_delta: bool,
  compressor_config: q_compress::CompressorConfig,
}

impl CodecInternal for QcoConfig {
  fn name(&self) -> &'static str {
    "qco"
  }

  fn get_confs(&self) -> Vec<(&'static str, String)> {
    vec![
      (
        "level",
        self.compressor_config.compression_level.to_string(),
      ),
      (
        "delta_order",
        if self.use_fixed_delta {
          self.compressor_config.delta_encoding_order.to_string()
        } else {
          "auto".to_string()
        },
      ),
      (
        "use_gcds",
        self.compressor_config.use_gcds.to_string(),
      ),
    ]
  }

  fn compress<T: PcoNumberLike>(&self, nums: &[T]) -> Vec<u8> {
    let mut c_config = self.compressor_config.clone();
    let qco_nums = T::nums_to_qco(nums);
    if !self.use_fixed_delta {
      c_config.delta_encoding_order =
        q_compress::auto_compressor_config(qco_nums, c_config.compression_level)
          .delta_encoding_order;
    }
    q_compress::standalone::Compressor::<T::Qco>::from_config(c_config).simple_compress(qco_nums)
  }

  fn decompress<T: PcoNumberLike>(&self, bytes: &[u8]) -> Vec<T> {
    let qco_nums = q_compress::auto_decompress::<T::Qco>(bytes).expect("could not decompress");
    T::qco_to_nums(qco_nums)
  }
}
