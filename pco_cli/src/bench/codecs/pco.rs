use anyhow::{anyhow, Result};

use pco::{FloatMultSpec, IntMultSpec, PagingSpec};

use crate::bench::codecs::CodecInternal;
use crate::dtypes::PcoNumberLike;

#[derive(Clone, Debug, Default)]
pub struct PcoConfig {
  chunk_config: pco::ChunkConfig,
}

impl CodecInternal for PcoConfig {
  fn name(&self) -> &'static str {
    "pco"
  }

  fn get_confs(&self) -> Vec<(&'static str, String)> {
    vec![
      (
        "level",
        self.chunk_config.compression_level.to_string(),
      ),
      (
        "delta_order",
        self
          .chunk_config
          .delta_encoding_order
          .map(|order| order.to_string())
          .unwrap_or("auto".to_string()),
      ),
      (
        "gcd",
        format!("{:?}", self.chunk_config.int_mult_spec),
      ),
      (
        "float_mult",
        format!("{:?}", self.chunk_config.float_mult_spec),
      ),
      (
        "chunk_n",
        match self.chunk_config.paging_spec {
          PagingSpec::EqualPagesUpTo(page_size) => page_size.to_string(),
          _ => panic!("unexpected paging spec"),
        },
      ),
    ]
  }

  fn set_conf(&mut self, key: &str, value: String) -> Result<()> {
    let value = value.to_lowercase();
    match key {
      "level" => self.chunk_config.compression_level = value.parse::<usize>().unwrap(),
      "delta_order" => {
        if let Ok(order) = value.parse::<usize>() {
          self.chunk_config.delta_encoding_order = Some(order);
        } else if value.to_lowercase() == "auto" {
          self.chunk_config.delta_encoding_order = None;
        } else {
          return Err(anyhow!(
            "cannot parse delta order: {}",
            value
          ));
        }
      }
      "gcd" => {
        self.chunk_config.int_mult_spec = match value.as_str() {
          "enabled" => IntMultSpec::Enabled,
          "disabled" => IntMultSpec::Disabled,
          other => match other.parse::<u64>() {
            Ok(mult) => IntMultSpec::Provided(mult),
            _ => return Err(anyhow!("cannot parse int mult: {}", other)),
          },
        }
      }
      "float_mult" => {
        self.chunk_config.float_mult_spec = match value.as_str() {
          "enabled" => FloatMultSpec::Enabled,
          "disabled" => FloatMultSpec::Disabled,
          other => match other.parse::<f64>() {
            Ok(mult) => FloatMultSpec::Provided(mult),
            _ => return Err(anyhow!("cannot parse float mult: {}", other)),
          },
        }
      }
      "chunk_n" => {
        self.chunk_config.paging_spec = PagingSpec::EqualPagesUpTo(value.parse().unwrap())
      }
      _ => return Err(anyhow!("unknown conf: {}", key)),
    }
    Ok(())
  }

  fn compress<T: PcoNumberLike>(&self, nums: &[T]) -> Vec<u8> {
    pco::standalone::simple_compress(nums, &self.chunk_config).expect("invalid config")
  }

  fn decompress<T: PcoNumberLike>(&self, bytes: &[u8]) -> Vec<T> {
    pco::standalone::simple_decompress::<T>(bytes).expect("could not decompress")
  }
}
