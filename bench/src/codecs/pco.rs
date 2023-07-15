use crate::codecs::CodecInternal;
use crate::dtypes::Dtype;
use anyhow::{anyhow, Result};

#[derive(Clone, Debug, Default)]
pub struct PcoConfig {
  compressor_config: pco::CompressorConfig,
}

impl CodecInternal for PcoConfig {
  fn name(&self) -> &'static str {
    "pco"
  }

  fn get_conf(&self, key: &str) -> String {
    match key {
      "level" => self.compressor_config.compression_level.to_string(),
      "delta_order" => self
        .compressor_config
        .delta_encoding_order
        .map(|order| order.to_string())
        .unwrap_or("auto".to_string()),
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
          self.compressor_config.delta_encoding_order = Some(order);
        } else if value.to_lowercase() == "auto" {
          self.compressor_config.delta_encoding_order = None;
        } else {
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
    let c_config = self.compressor_config.clone();
    let pco_nums = T::slice_to_pco(nums);
    pco::standalone::simple_compress(pco_nums, c_config).expect("invalid config")
  }

  fn decompress<T: Dtype>(&self, bytes: &[u8]) -> Vec<T> {
    let v = pco::standalone::auto_decompress::<T::Pco>(bytes).expect("could not decompress");
    T::vec_from_pco(v)
  }
}
