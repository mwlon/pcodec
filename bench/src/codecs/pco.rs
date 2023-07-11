use crate::codecs::CodecInternal;
use crate::dtypes::Dtype;
use anyhow::{anyhow, Result};

#[derive(Clone, Debug, Default)]
pub struct PcoConfig {
  use_fixed_delta: bool,
  compressor_config: pco::CompressorConfig,
}

impl CodecInternal for PcoConfig {
  fn name(&self) -> &'static str {
    "pco"
  }

  fn get_conf(&self, key: &str) -> String {
    match key {
      "level" => self.compressor_config.compression_level.to_string(),
      "delta_order" => {
        if self.use_fixed_delta {
          self.compressor_config.delta_encoding_order.to_string()
        } else {
          "auto".to_string()
        }
      }
      "use_gcds" => self.compressor_config.use_gcds.to_string(),
      "use_float_mult" => self.compressor_config.use_float_mult.to_string(),
      _ => panic!("bad conf"),
    }
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
      "use_float_mult" => self.compressor_config.use_float_mult = value.parse::<bool>().unwrap(),
      _ => return Err(anyhow!("unknown conf: {}", key)),
    }
    Ok(())
  }

  fn compress<T: Dtype>(&self, nums: &[T]) -> Vec<u8> {
    let mut c_config = self.compressor_config.clone();
    let pco_nums = T::slice_to_pco(nums);
    if !self.use_fixed_delta {
      c_config.delta_encoding_order =
        pco::auto_compressor_config(pco_nums, c_config.compression_level).delta_encoding_order;
    }
    pco::standalone::simple_compress(c_config, pco_nums)
  }

  fn decompress<T: Dtype>(&self, bytes: &[u8]) -> Vec<T> {
    let v = pco::standalone::auto_decompress::<T::Pco>(bytes).expect("could not decompress");
    T::vec_from_pco(v)
  }
}
