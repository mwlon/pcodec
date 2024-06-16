use pco::{ChunkConfig, FloatMultSpec, FloatQuantSpec, IntMultSpec};

use crate::bench::codecs::CodecInternal;
use crate::chunk_config_opt::ChunkConfigOpt;
use crate::dtypes::PcoNumberLike;

fn unparse_int_mult(spec: &IntMultSpec) -> String {
  use IntMultSpec::*;
  match spec {
    Disabled => "Disabled".to_string(),
    Enabled => "Enabled".to_string(),
    Provided(base) => base.to_string(),
  }
}

fn unparse_float_mult(spec: &FloatMultSpec) -> String {
  use FloatMultSpec::*;
  match spec {
    Disabled => "Disabled".to_string(),
    Enabled => "Enabled".to_string(),
    Provided(base) => base.to_string(),
  }
}

fn unparse_float_quant(spec: &FloatQuantSpec) -> String {
  use FloatQuantSpec::*;
  match spec {
    Disabled => "Disabled".to_string(),
    Provided(k) => k.to_string(),
  }
}

impl CodecInternal for ChunkConfigOpt {
  fn name(&self) -> &'static str {
    "pco"
  }

  fn get_confs(&self) -> Vec<(&'static str, String)> {
    vec![
      ("level", self.level.to_string()),
      (
        "delta-order",
        self
          .delta_encoding_order()
          .map(|order| order.to_string())
          .unwrap_or("Auto".to_string()),
      ),
      ("int-mult", unparse_int_mult(&self.int_mult)),
      (
        "float-mult",
        unparse_float_mult(&self.float_mult),
      ),
      (
        "float-quant",
        unparse_float_quant(&self.float_quant),
      ),
      ("chunk-n", self.chunk_n.to_string()),
    ]
  }

  fn compress<T: PcoNumberLike>(&self, nums: &[T]) -> Vec<u8> {
    let chunk_config = ChunkConfig::from(self);
    pco::standalone::simple_compress(nums, &chunk_config).expect("invalid config")
  }

  fn decompress<T: PcoNumberLike>(&self, bytes: &[u8]) -> Vec<T> {
    pco::standalone::simple_decompress::<T>(bytes).expect("could not decompress")
  }
}
