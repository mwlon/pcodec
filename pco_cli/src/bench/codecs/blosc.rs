use std::ffi::{c_int, c_void, CString};
use std::mem;

use anyhow::anyhow;

use crate::bench::codecs::CodecInternal;
use crate::dtypes::PcoNumberLike;

#[derive(Clone, Debug)]
pub struct BloscConfig {
  block_size: usize,
  cname: String,
  clevel: c_int,
}

impl Default for BloscConfig {
  fn default() -> Self {
    Self {
      block_size: 1 << 20,
      cname: "blosclz".to_string(),
      clevel: 9,
    }
  }
}

impl CodecInternal for BloscConfig {
  fn name(&self) -> &'static str {
    "blosc"
  }

  fn get_confs(&self) -> Vec<(&'static str, String)> {
    vec![
      ("block_size", self.block_size.to_string()),
      ("cname", self.cname.to_string()),
      ("level", self.clevel.to_string()),
    ]
  }

  fn set_conf(&mut self, key: &str, value: String) -> anyhow::Result<()> {
    match key {
      "block_size" => self.block_size = value.parse()?,
      "cname" => self.cname = value,
      "level" => self.clevel = value.parse()?,
      _ => return Err(anyhow!("unknown blosc key: {}", key)),
    }
    Ok(())
  }

  fn compress<T: PcoNumberLike>(&self, nums: &[T]) -> Vec<u8> {
    let type_size = mem::size_of::<T>();
    let n_bytes = std::mem::size_of_val(nums);
    let mut dst = Vec::with_capacity(n_bytes + blosc_src::BLOSC_MAX_OVERHEAD as usize);
    unsafe {
      let src = nums.as_ptr() as *const c_void;
      let compressor_name = CString::new(self.cname.to_string()).unwrap();
      let compressed_size = blosc_src::blosc_compress_ctx(
        self.clevel,
        blosc_src::BLOSC_SHUFFLE as c_int,
        type_size,
        n_bytes,
        src,
        dst.as_mut_ptr() as *mut c_void,
        dst.capacity(),
        compressor_name.as_ptr(),
        self.block_size,
        1,
      );
      dst.set_len(compressed_size as usize);
    }
    dst
  }

  fn decompress<T: PcoNumberLike>(&self, compressed: &[u8]) -> Vec<T> {
    let mut uncompressed_size = 0;
    let mut compressed_size = 0_usize;
    let mut block_size = 0_usize;
    unsafe {
      let src = compressed.as_ptr() as *const c_void;
      blosc_src::blosc_cbuffer_sizes(
        src,
        &mut uncompressed_size as *mut usize,
        &mut compressed_size as *mut usize,
        &mut block_size as *mut usize,
      );
      let n = uncompressed_size / mem::size_of::<T>();
      let mut res = Vec::with_capacity(n);
      let dst = res.as_mut_ptr() as *mut c_void;
      blosc_src::blosc_decompress(src, dst, uncompressed_size);
      res.set_len(n);
      res
    }
  }
}
