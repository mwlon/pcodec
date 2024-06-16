use clap::{ArgMatches, CommandFactory, FromArgMatches};

use pco::ChunkConfig;

use crate::bench::codecs::CodecInternal;
use crate::chunk_config_opt::ChunkConfigOpt;
use crate::dtypes::PcoNumberLike;

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
          .unwrap_or("auto".to_string()),
      ),
      ("int-mult", format!("{:?}", self.int_mult)),
      (
        "float-mult",
        format!("{:?}", self.float_mult),
      ),
      (
        "float-quant",
        format!("{:?}", self.float_quant),
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
