use anyhow::{anyhow, Result};

use crate::bench::codecs::CodecInternal;
use crate::dtypes::PcoNumberLike;

#[derive(Clone, Debug, Default)]
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

  fn set_conf(&mut self, key: &str, value: String) -> Result<()> {
    match key {
      "level" => self.compressor_config.compression_level = value.parse::<usize>().unwrap(),
      "delta_order" => {
        if let Ok(order) = value.parse::<usize>() {
          self.compressor_config.delta_encoding_order = order;
          self.use_fixed_delta = true;
        } else if value.to_lowercase() != "auto" {
          return Err(anyhow!(
            "cannot parse delta order: {}",
            value
          ));
        }
      }
      "use_gcds" => self.compressor_config.use_gcds = value.parse::<bool>().unwrap(),
      _ => return Err(anyhow!("unknown conf: {}", key)),
    }
    Ok(())
  }

  fn compress<T: PcoNumberLike>(&self, nums: &[T]) -> Vec<u8> {
    let mut c_config = self.compressor_config.clone();
    if !self.use_fixed_delta {
      c_config.delta_encoding_order =
        q_compress::auto_compressor_config(nums, c_config.compression_level).delta_encoding_order;
    }
    q_compress::standalone::Compressor::<T>::from_config(c_config).simple_compress(nums)
  }

  fn decompress<T: PcoNumberLike>(&self, bytes: &[u8]) -> Vec<T> {
    q_compress::auto_decompress::<T>(bytes).expect("could not decompress")
  }
}
