use libc::{c_uchar, c_uint, c_void};
use paste::paste;
use pco::data_types::NumberLike;

#[repr(C)]
pub enum PcoError {
  Success,
  InvalidType,
  DecompressionError,
}

macro_rules! ffi_types {
  {$($names:ident => $types:ty,)+} => {
    // TODO: the following constants must be manually fixed in the cbindgen header output.
    paste! {$(pub const [<PCO_TYPE_ $names>]: c_uchar = <$types>::DTYPE_BYTE;)+ }

    enum FfiVec {
      U8(Vec<u8>),
      $($names(Vec<$types>),)+
    }

    trait IntoFfiVec {
      fn into_ffi_vec(self) -> FfiVec;
    }

    impl IntoFfiVec for Vec<u8> {
      fn into_ffi_vec(self) -> FfiVec { FfiVec::U8(self) }
    }

    $(
      impl IntoFfiVec for Vec<$types> {
        fn into_ffi_vec(self) -> FfiVec { FfiVec::$names(self) }
      }
    )+

    macro_rules! ffi_switch {
      ($matcher:expr, $fn:ident, $params:tt) =>
      {
        match $matcher {
          $(<$types>::DTYPE_BYTE => $fn::<$types>$params,)+
         _ => return PcoError::InvalidType
      }
      }
    }
  }
}

ffi_types!(
  U32 => u32,
  U64 => u64,
  I32 => i32,
  I64 => i64,
  F32 => f32,
  F64 => f64,
);

#[repr(C)]
pub struct PcoVec {
  ptr: *const c_void,
  len: c_uint,
  raw_box: *const c_void,
}

impl PcoVec {
  fn init_from_vec<T>(&mut self, v: Vec<T>)
  where
    T: Sized,
    Vec<T>: IntoFfiVec,
  {
    self.ptr = v.as_ptr() as *const c_void;
    self.len = v.len() as c_uint;
    self.raw_box = Box::into_raw(Box::new(v.into_ffi_vec())) as *const c_void;
  }

  fn free(&self) {
    unsafe { drop(Box::from_raw(self.raw_box as *mut FfiVec)) }
  }
}

fn _auto_compress<T: NumberLike>(
  nums: *const c_void,
  len: c_uint,
  level: c_uint,
  pco_vec: *mut c_void,
) -> PcoError {
  let slice = unsafe { std::slice::from_raw_parts(nums as *const T, len as usize) };
  let c_struct: &mut PcoVec = unsafe { &mut *(pco_vec as *mut PcoVec) };
  let v = pco::standalone::auto_compress(slice, level as usize);
  c_struct.init_from_vec(v);
  PcoError::Success
}

fn _auto_decompress<T: NumberLike>(
  compressed: *const c_void,
  len: c_uint,
  pco_vec: *mut c_void,
) -> PcoError
where
  T: Sized,
  Vec<T>: IntoFfiVec,
{
  let slice = unsafe { std::slice::from_raw_parts(compressed as *const u8, len as usize) };
  let c_struct: &mut PcoVec = unsafe { &mut *(pco_vec as *mut PcoVec) };
  match pco::standalone::auto_decompress::<T>(slice) {
    Err(_) => PcoError::DecompressionError,
    Ok(v) => {
      c_struct.init_from_vec(v);
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
  pco_vec: *mut c_void,
) -> PcoError {
  ffi_switch!(
    dtype,
    _auto_compress,
    (nums, len, level, pco_vec)
  )
}

#[no_mangle]
pub extern "C" fn auto_decompress(
  compressed: *const c_void,
  len: c_uint,
  dtype: c_uchar,
  pco_vec: *mut c_void,
) -> PcoError {
  ffi_switch!(
    dtype,
    _auto_decompress,
    (compressed, len, pco_vec)
  )
}

#[no_mangle]
pub extern "C" fn free_pcovec(pco_vec: *mut PcoVec) -> PcoError {
  unsafe { (*pco_vec).free() };
  PcoError::Success
}
