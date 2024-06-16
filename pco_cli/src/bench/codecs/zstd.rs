use std::convert::TryInto;

use clap::Parser;

use crate::bench::codecs::{utils, CodecInternal};
use crate::dtypes::PcoNumberLike;

#[derive(Clone, Debug, Default, Parser)]
pub struct ZstdConfig {
  #[arg(long)]
  level: i32,
}

impl CodecInternal for ZstdConfig {
  fn name(&self) -> &'static str {
    "zstd"
  }

  fn get_confs(&self) -> Vec<(&'static str, String)> {
    vec![("level", self.level.to_string())]
  }

  fn compress<T: PcoNumberLike>(&self, nums: &[T]) -> Vec<u8> {
    let mut res = Vec::new();
    res.extend((nums.len() as u32).to_le_bytes());
    unsafe {
      zstd::stream::copy_encode(
        utils::num_slice_to_bytes(nums),
        &mut res,
        self.level,
      )
      .unwrap();
    }
    res
  }

  fn decompress<T: PcoNumberLike>(&self, bytes: &[u8]) -> Vec<T> {
    let len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let mut res = Vec::<T>::with_capacity(len);
    unsafe {
      res.set_len(len);
      zstd::stream::copy_decode(
        &bytes[4..],
        utils::num_slice_to_bytes_mut(res.as_mut_slice()),
      )
      .unwrap();
    }
    res
  }
}
