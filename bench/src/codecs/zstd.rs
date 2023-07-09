use std::convert::TryInto;

use anyhow::{anyhow, Result};

use crate::codecs::{utils, CodecInternal};
use crate::NumberLike;

#[derive(Clone, Debug, Default)]
pub struct ZstdConfig {
  level: i32,
}

impl CodecInternal for ZstdConfig {
  fn name(&self) -> &'static str {
    "zstd"
  }

  fn get_conf(&self, key: &str) -> String {
    match key {
      "level" => self.level.to_string(),
      _ => panic!("bad conf"),
    }
  }

  fn set_conf(&mut self, key: &str, value: String) -> Result<()> {
    match key {
      "level" => self.level = value.parse::<i32>().unwrap(),
      _ => return Err(anyhow!("unknown conf: {}", key)),
    }
    Ok(())
  }

  // we prefix with a u32 of the
  fn compress<T: NumberLike>(&self, nums: &[T]) -> Vec<u8> {
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

  fn decompress<T: NumberLike>(&self, bytes: &[u8]) -> Vec<T> {
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
