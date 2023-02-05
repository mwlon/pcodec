use libc::c_uint;

use q_compress::data_types::NumberLike;

#[repr(C)]
pub struct FfiVec<T: Sized> {
  raw_box: *mut Vec<T>,
  ptr: *const T,
  len: c_uint, // necessary for C process to know length of Rust Vec
}

impl<T: Sized> FfiVec<T> {
  pub fn from_vec(v: Vec<T>) -> Self {
    let len = v.len() as c_uint;
    let ptr = v.as_ptr();
    FfiVec {
      raw_box: Box::into_raw(Box::new(v)),
      ptr,
      len,
    }
  }

  pub fn free(self) {
    unsafe { drop(Box::from_raw(self.raw_box)) }
  }
}

fn auto_compress<T: NumberLike>(nums: *const T, len: c_uint, level: c_uint) -> FfiVec<u8> {
  let slice = unsafe { std::slice::from_raw_parts(nums, len as usize) };
  FfiVec::from_vec(q_compress::auto_compress(
    slice,
    level as usize,
  ))
}

fn auto_decompress<T: NumberLike>(compressed: *const u8, len: c_uint) -> FfiVec<T> {
  let slice = unsafe { std::slice::from_raw_parts(compressed, len as usize) };
  let decompressed = q_compress::auto_decompress::<T>(slice).expect("decompression error!"); // TODO surface error string in CVec instead of panicking
  FfiVec::from_vec(decompressed)
}

#[no_mangle]
pub extern "C" fn auto_compress_i32(nums: *const i32, len: c_uint, level: c_uint) -> FfiVec<u8> {
  auto_compress(nums, len, level)
}

#[no_mangle]
pub extern "C" fn free_compressed(ffi_vec: FfiVec<u8>) {
  ffi_vec.free()
}

#[no_mangle]
pub extern "C" fn auto_decompress_i32(compressed: *mut u8, len: c_uint) -> FfiVec<i32> {
  auto_decompress::<i32>(compressed, len)
}

#[no_mangle]
pub extern "C" fn free_i32(ffi_vec: FfiVec<i32>) {
  ffi_vec.free()
}
