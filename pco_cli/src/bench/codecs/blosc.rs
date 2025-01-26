use std::ffi::{c_void, CString};
use std::mem;

use crate::bench::codecs::CodecInternal;
use crate::dtypes::PcoNumber;
use clap::Parser;

#[derive(Clone, Debug, Parser)]
pub struct BloscConfig {
  #[arg(long, default_value = "1048576")]
  block_size: i32,
  #[arg(long, default_value = "blosclz")]
  cname: String,
  #[arg(long, default_value = "9")]
  clevel: u8,
}

impl BloscConfig {
  unsafe fn create_ctx(&self, typesize: i32) -> *mut blosc2_src::blosc2_context {
    let compressor_name = CString::new(self.cname.to_string()).unwrap();
    let compcode = blosc2_src::blosc2_compname_to_compcode(compressor_name.as_ptr());
    let mut filters = [0; 6]; // no filters
    filters[0] = 1; // byte shuffle
    let cparams = blosc2_src::blosc2_cparams {
      compcode: compcode as u8,
      compcode_meta: 0,
      clevel: self.clevel,
      use_dict: 0,
      typesize,
      nthreads: 1,
      blocksize: self.block_size,
      splitmode: 0,
      schunk: std::ptr::null_mut(),
      filters,
      filters_meta: [0; 6],
      prefilter: None,
      preparams: std::ptr::null_mut(),
      tuner_params: std::ptr::null_mut(),
      tuner_id: 0,
      instr_codec: false,
      codec_params: std::ptr::null_mut(),
      filter_params: [std::ptr::null_mut(); 6],
    };
    blosc2_src::blosc2_create_cctx(cparams)
  }

  unsafe fn create_dctx(&self) -> *mut blosc2_src::blosc2_context {
    let dparams = blosc2_src::blosc2_dparams {
      nthreads: 1,
      schunk: std::ptr::null_mut(),
      postfilter: None,
      postparams: std::ptr::null_mut(),
    };
    blosc2_src::blosc2_create_dctx(dparams)
  }
}

impl CodecInternal for BloscConfig {
  fn name(&self) -> &'static str {
    "blosc"
  }

  fn get_confs(&self) -> Vec<(&'static str, String)> {
    vec![
      ("block-size", self.block_size.to_string()),
      ("cname", self.cname.to_string()),
      ("level", self.clevel.to_string()),
    ]
  }

  fn compress<T: PcoNumber>(&self, nums: &[T]) -> Vec<u8> {
    let n_bytes = mem::size_of_val(nums);
    let mut dst = Vec::with_capacity(n_bytes + blosc2_src::BLOSC2_MAX_OVERHEAD as usize);
    unsafe {
      let src = nums.as_ptr() as *const c_void;
      let ctx = self.create_ctx(mem::size_of::<T>() as i32);
      let compressed_size = blosc2_src::blosc2_compress_ctx(
        ctx,
        src,
        n_bytes as i32,
        dst.as_mut_ptr() as *mut c_void,
        dst.capacity() as i32,
      );
      dst.set_len(compressed_size as usize);
      blosc2_src::blosc2_free_ctx(ctx);
    }
    dst
  }

  fn decompress<T: PcoNumber>(&self, compressed: &[u8]) -> Vec<T> {
    let mut uncompressed_size = 0;
    let mut compressed_size = 0;
    let mut block_size = 0;
    unsafe {
      let src = compressed.as_ptr() as *const c_void;
      let ctx = self.create_dctx();
      blosc2_src::blosc2_cbuffer_sizes(
        src,
        &mut uncompressed_size as *mut i32,
        &mut compressed_size as *mut i32,
        &mut block_size as *mut i32,
      );
      let n = uncompressed_size as usize / mem::size_of::<T>();
      let mut res = Vec::with_capacity(n);
      let dst = res.as_mut_ptr() as *mut c_void;
      blosc2_src::blosc2_decompress_ctx(
        ctx,
        src,
        compressed.len() as i32,
        dst,
        uncompressed_size,
      );
      blosc2_src::blosc2_free_ctx(ctx);
      res.set_len(n);
      res
    }
  }
}
