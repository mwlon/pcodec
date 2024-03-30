use crate::bench::codecs::CodecInternal;
use crate::bench::dtypes::Dtype;
use std::ffi::{c_int, c_void, CString};

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

  fn get_conf(&self, key: &str) -> String {
    match key {
      "block_size" => self.block_size.to_string(),
      "cname" => self.cname.to_string(),
      "level" => self.clevel.to_string(),
      _ => panic!("unknown blosc key: {}", key),
    }
  }

  fn set_conf(&mut self, key: &str, value: String) -> anyhow::Result<()> {
    match key {
      "block_size" => self.block_size = value.parse()?,
      "cname" => self.cname = value,
      "level" => self.clevel = value.parse()?,
      _ => panic!("unknown blosc key: {}", key),
    }
    Ok(())
  }

  fn compress<T: Dtype>(&self, nums: &[T]) -> Vec<u8> {
    let type_size = T::PHYSICAL_BITS / 8;
    let n_bytes = nums.len() * type_size;
    let mut dst = Vec::with_capacity(n_bytes + blosc_src::BLOSC_MAX_OVERHEAD as usize);
    unsafe {
      let src = nums.as_ptr() as *const c_void;
      let compressor_name = CString::new(self.cname.to_string()).unwrap();
      let compressed_size = blosc_src::blosc_compress_ctx(
        self.clevel,
        blosc_src::BLOSC_SHUFFLE as c_int,
        T::PHYSICAL_BITS / 8,
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

  fn decompress<T: Dtype>(&self, compressed: &[u8]) -> Vec<T> {
    let type_size = T::PHYSICAL_BITS / 8;

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
      let n = uncompressed_size / type_size;
      let mut res = Vec::with_capacity(n);
      let dst = res.as_mut_ptr() as *mut c_void;
      blosc_src::blosc_decompress(src, dst, uncompressed_size);
      res.set_len(n);
      res
    }
  }
}
