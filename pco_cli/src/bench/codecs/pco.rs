use pco::{ChunkConfig, ModeSpec};

use crate::bench::codecs::CodecInternal;
use crate::chunk_config_opt::ChunkConfigOpt;
use crate::dtypes::PcoNumberLike;

fn unparse_mode_spec(spec: &ModeSpec) -> String {
  match spec {
    ModeSpec::Auto => "Auto".to_string(),
    ModeSpec::Classic => "Classic".to_string(),
    ModeSpec::TryFloatMult(base) => format!("FloatMult@{}", base),
    ModeSpec::TryFloatQuant(k) => format!("FloatQuant@{}", k),
    ModeSpec::TryIntMult(base) => format!("IntMult@{}", base),
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
          .delta_encoding_order
          .map(|order| order.to_string())
          .unwrap_or("Auto".to_string()),
      ),
      ("mode", unparse_mode_spec(&self.mode)),
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
