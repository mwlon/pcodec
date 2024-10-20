#![allow(clippy::missing_safety_doc)]

use std::ptr;

use libc::{c_uchar, c_uint, c_void, size_t};

use pco::data_types::{CoreDataType, NumberLike};

use crate::PcoError::PcoInvalidType;

#[repr(C)]
pub enum PcoError {
  PcoSuccess,
  PcoInvalidType,
  // TODO split this into the actual error kinds
  PcoCompressionError,
  PcoDecompressionError,
}

macro_rules! impl_dtypes {
  {$($names:ident($lname:ident) => $types:ty,)+} => {
    // tell rust to ignore these warnings because we rely on the memory layout in C
    #[allow(dead_code)]
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

pco::with_core_dtypes!(impl_dtypes);

#[repr(C)]
pub struct PcoFfiVec {
  ptr: *const c_void,
  len: size_t,
  raw_box: *const c_void,
}

impl PcoFfiVec {
  fn init_from_vec<T>(&mut self, v: Vec<T>)
  where
    Vec<T>: Into<DynTypedVec>,
  {
    self.ptr = v.as_ptr() as *const c_void;
    self.len = v.len();
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

fn _simpler_compress<T: NumberLike>(
  nums: *const c_void,
  len: size_t,
  level: c_uint,
  ffi_vec_ptr: *mut PcoFfiVec,
) -> PcoError {
  let slice = unsafe { std::slice::from_raw_parts(nums as *const T, len) };
  match pco::standalone::simpler_compress(slice, level as usize) {
    Err(_) => PcoError::PcoCompressionError,
    Ok(v) => {
      unsafe { (*ffi_vec_ptr).init_from_vec(v) };
      PcoError::PcoSuccess
    }
  }
}

fn _simple_decompress<T: NumberLike>(
  compressed: *const c_void,
  len: size_t,
  ffi_vec_ptr: *mut PcoFfiVec,
) -> PcoError
where
  Vec<T>: Into<DynTypedVec>,
{
  let slice = unsafe { std::slice::from_raw_parts(compressed as *const u8, len) };
  match pco::standalone::simple_decompress::<T>(slice) {
    Err(_) => PcoError::PcoDecompressionError,
    Ok(v) => {
      unsafe { (*ffi_vec_ptr).init_from_vec(v) };
      PcoError::PcoSuccess
    }
  }
}

#[no_mangle]
pub extern "C" fn pco_simpler_compress(
  nums: *const c_void,
  len: size_t,
  dtype: c_uchar,
  level: c_uint,
  dst: *mut PcoFfiVec,
) -> PcoError {
  let Some(dtype) = CoreDataType::from_descriminant(dtype) else {
    return PcoInvalidType;
  };

  match_dtype!(
    dtype,
    _simpler_compress,
    (nums, len, level, dst)
  )
}

#[no_mangle]
pub extern "C" fn pco_simple_decompress(
  compressed: *const c_void,
  len: size_t,
  dtype: c_uchar,
  dst: *mut PcoFfiVec,
) -> PcoError {
  let Some(dtype) = CoreDataType::from_descriminant(dtype) else {
    return PcoInvalidType;
  };

  match_dtype!(
    dtype,
    _simple_decompress,
    (compressed, len, dst)
  )
}

#[no_mangle]
pub unsafe extern "C" fn pco_free_pcovec(ffi_vec: *mut PcoFfiVec) -> PcoError {
  unsafe { (*ffi_vec).free() };
  PcoError::PcoSuccess
}
