use std::convert::TryInto;
use std::io::{Read, Write};

use anyhow::{anyhow, Result};

use crate::bench::codecs::{utils, CodecInternal};
use crate::bench::dtypes::Dtype;
use crate::dtypes::PcoNumberLike;

#[derive(Clone, Debug, Default)]
pub struct SnappyConfig {}

impl CodecInternal for SnappyConfig {
  fn name(&self) -> &'static str {
    "snappy"
  }

  fn get_confs(&self) -> Vec<(&'static str, String)> {
    vec![]
  }

  fn set_conf(&mut self, key: &str, _value: String) -> Result<()> {
    Err(anyhow!("unknown conf: {}", key))
  }

  fn compress<T: PcoNumberLike>(&self, nums: &[T]) -> Vec<u8> {
    let mut res = Vec::new();
    res.extend((nums.len() as u32).to_le_bytes());

    unsafe {
      let mut wtr = snap::write::FrameEncoder::new(&mut res);
      wtr.write_all(utils::num_slice_to_bytes(nums)).unwrap();
      wtr.flush().unwrap();
    }
    res
  }

  fn decompress<T: PcoNumberLike>(&self, bytes: &[u8]) -> Vec<T> {
    let len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let mut res = Vec::<T>::with_capacity(len);
    let mut rdr = snap::read::FrameDecoder::new(&bytes[4..]);
    unsafe {
      res.set_len(len);
      rdr
        .read_exact(utils::num_slice_to_bytes_mut(
          res.as_mut_slice(),
        ))
        .unwrap();
    }
    res
  }
}
