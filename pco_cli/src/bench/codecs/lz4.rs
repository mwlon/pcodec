use crate::bench::codecs::{utils, CodecInternal};
use crate::dtypes::PcoNumber;
use clap::Parser;
use lz4::{Decoder, EncoderBuilder};
use std::convert::TryInto;
use std::io;
use std::ptr::null;

#[derive(Clone, Debug, Parser)]
pub struct Lz4Config {
  #[arg(long, default_value = "0")]
  level: u32,
}

impl CodecInternal for Lz4Config {
  fn name(&self) -> &'static str {
    "lz4"
  }

  fn get_confs(&self) -> Vec<(&'static str, String)> {
    vec![("level", self.level.to_string())]
  }

  fn compress<T: PcoNumber>(&self, nums: &[T]) -> Vec<u8> {
    unsafe {
      let mut src = utils::num_slice_to_bytes(nums);
      let dst_capacity = 4 + lz4::liblz4::LZ4F_compressBound(src.len(), null());
      let mut res = Vec::with_capacity(dst_capacity);
      res.extend((nums.len() as u32).to_le_bytes());
      res.set_len(dst_capacity + 4);
      let mut encoder = EncoderBuilder::new()
        .level(self.level)
        .build(&mut res[4..])
        .unwrap();
      io::copy(&mut src, &mut encoder).unwrap();
      let (_, result) = encoder.finish();
      result.unwrap();
      res
    }
  }

  fn decompress<T: PcoNumber>(&self, bytes: &[u8]) -> Vec<T> {
    let len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let mut res = Vec::<T>::with_capacity(len);
    unsafe {
      res.set_len(len);
      let mut decoder = Decoder::new(&bytes[4..]).unwrap();
      let mut dst = utils::num_slice_to_bytes_mut(&mut res);
      io::copy(&mut decoder, &mut dst).unwrap();
    }
    res
  }
}
