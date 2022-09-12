use std::alloc::Layout;

use libc::c_uint;

use q_compress::data_types::NumberLike;

#[repr(C)]
pub struct CVec<T: Sized> {
  ptr: *mut Vec<T>,
  len: c_uint, // necessary for C process to know length of Rust Vec
}

impl<T: Sized> CVec<T> {
  pub fn from_vec(v: Vec<T>) -> Self {
    let len = v.len() as c_uint;
    CVec {
      ptr: Box::into_raw(Box::new(v)),
      len,
    }
  }

  pub fn slice(&self) -> &[T] {
    unsafe { &*self.ptr }
  }

  pub fn drop(self) {
    unsafe {
      std::ptr::drop_in_place(self.ptr);
      std::alloc::dealloc(self.ptr as *mut u8, Layout::new::<Vec<T>>())
    }
  }
}

fn auto_compress<T: NumberLike>(
  nums: *const T,
  len: c_uint,
  level: c_uint,
) -> CVec<u8> {
  let slice = unsafe { std::slice::from_raw_parts(nums, len as usize)};
  CVec::from_vec(q_compress::auto_compress(slice, level as usize))
}

fn auto_decompress<T: NumberLike>(
  compressed: *const u8,
  len: c_uint,
) -> CVec<T> {
  let slice = unsafe { std::slice::from_raw_parts(compressed, len as usize)};
  let decompressed = q_compress::auto_decompress::<T>(slice)
    .expect("decompression error!"); // TODO surface error string in CVec instead of panicking
  CVec::from_vec(decompressed)
}

// assumes buffer is the right size, so C process needs to read length of Rust Vec and pass
// an appropriate buffer
fn move_into_buffer<T: Copy>(
  c_vec: CVec<T>,
  buffer: *mut T,
) {
  let len = c_vec.len as usize;
  let buffer_slice = unsafe { std::slice::from_raw_parts_mut(buffer, len) };
  for (i, &item) in c_vec.slice().iter().take(len).enumerate() {
    buffer_slice[i] = item;
  }
  c_vec.drop()
}

#[no_mangle]
pub extern "C" fn auto_compress_i32(
  nums: *const i32,
  len: c_uint,
  level: c_uint,
) -> CVec<u8> {
  auto_compress(nums, len, level)
}

#[no_mangle]
pub extern "C" fn move_compressed_into_buffer(
  c_vec: CVec<u8>,
  buffer: *mut u8,
) {
  move_into_buffer(c_vec, buffer)
}

#[no_mangle]
pub extern "C" fn auto_decompress_i32(
  compressed: *mut u8,
  len: c_uint,
) -> CVec<i32> {
  auto_decompress::<i32>(compressed, len)
}

#[no_mangle]
pub extern "C" fn move_i32_into_buffer(
  c_vec: CVec<i32>,
  buffer: *mut i32,
) {
  move_into_buffer(c_vec, buffer)
}

