#![allow(clippy::missing_safety_doc)]

use std::ptr;

use libc::{c_uchar, c_uint, c_void};

use pco::data_types::{CoreDataType, NumberLike};

use crate::PcoError::InvalidType;

#[repr(C)]
pub enum PcoError {
  Success,
  InvalidType,
  DecompressionError, // TODO split this into the actual error kinds
}

macro_rules! impl_dtypes {
  {$($names:ident => $types:ty,)+} => {
    enum DynTypedVec {
      U8(Vec<u8>),
      $($names(Vec<$types>),)+
    }

    impl From<Vec<u8>> for DynTypedVec {
      fn from(vec: Vec<u8>) -> DynTypedVec { DynTypedVec::U8(vec) }
    }

    $(
      impl From<Vec<$types>> for DynTypedVec {
        fn from(vec: Vec<$types>) -> DynTypedVec { DynTypedVec::$names(vec) }
      }
    )+

    macro_rules! match_dtype {
      ($matcher:expr, $fn:ident, $params:tt) => {
        match $matcher {
          $(CoreDataType::$names => $fn::<$types>$params,)+
        }
      }
    }
  }
}

impl_dtypes!(
  U32 => u32,
  U64 => u64,
  I32 => i32,
  I64 => i64,
  F32 => f32,
  F64 => f64,
);

#[repr(C)]
pub struct PcoFfiVec {
  ptr: *const c_void,
  len: c_uint,
  raw_box: *const c_void,
}

impl PcoFfiVec {
  fn init_from_vec<T>(&mut self, v: Vec<T>)
  where
    Vec<T>: Into<DynTypedVec>,
  {
    self.ptr = v.as_ptr() as *const c_void;
    self.len = v.len() as c_uint;
    self.raw_box = Box::into_raw(Box::new(v.into())) as *const c_void;
  }

  fn free(&mut self) {
    unsafe {
      drop(Box::from_raw(
        self.raw_box as *mut DynTypedVec,
      ));
    }
    self.ptr = ptr::null();
    self.len = 0;
    self.raw_box = ptr::null();
  }
}

fn _auto_compress<T: NumberLike>(
  nums: *const c_void,
  len: c_uint,
  level: c_uint,
  ffi_vec_ptr: *mut PcoFfiVec,
) -> PcoError {
  let slice = unsafe { std::slice::from_raw_parts(nums as *const T, len as usize) };
  let v = pco::standalone::auto_compress(slice, level as usize);
  unsafe { (*ffi_vec_ptr).init_from_vec(v) };
  PcoError::Success
}

fn _auto_decompress<T: NumberLike>(
  compressed: *const c_void,
  len: c_uint,
  ffi_vec_ptr: *mut PcoFfiVec,
) -> PcoError
where
  Vec<T>: Into<DynTypedVec>,
{
  let slice = unsafe { std::slice::from_raw_parts(compressed as *const u8, len as usize) };
  match pco::standalone::auto_decompress::<T>(slice) {
    Err(_) => PcoError::DecompressionError,
    Ok(v) => {
      unsafe { (*ffi_vec_ptr).init_from_vec(v) };
      PcoError::Success
    }
  }
}

#[no_mangle]
pub extern "C" fn auto_compress(
  nums: *const c_void,
  len: c_uint,
  dtype: c_uchar,
  level: c_uint,
  dst: *mut PcoFfiVec,
) -> PcoError {
  let Some(dtype) = CoreDataType::from_byte(dtype) else {
    return InvalidType;
  };

  match_dtype!(
    dtype,
    _auto_compress,
    (nums, len, level, dst)
  )
}

#[no_mangle]
pub extern "C" fn auto_decompress(
  compressed: *const c_void,
  len: c_uint,
  dtype: c_uchar,
  dst: *mut PcoFfiVec,
) -> PcoError {
  let Some(dtype) = CoreDataType::from_byte(dtype) else {
    return InvalidType;
  };

  match_dtype!(
    dtype,
    _auto_decompress,
    (compressed, len, dst)
  )
}

#[no_mangle]
pub unsafe extern "C" fn free_pcovec(ffi_vec: *mut PcoFfiVec) -> PcoError {
  unsafe { (*ffi_vec).free() };
  PcoError::Success
}
