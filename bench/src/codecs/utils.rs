use std::mem;

use crate::dtypes::Dtype;

// cursed ways to convert nums to bytes and back without doing work
pub unsafe fn num_slice_to_bytes<T: Dtype>(slice: &[T]) -> &[u8] {
  let len = slice.len();
  let byte_len = len * (T::PHYSICAL_BITS / 8);
  &*std::ptr::slice_from_raw_parts(
    mem::transmute::<_, *const u8>(slice.as_ptr()),
    byte_len,
  )
}

pub unsafe fn num_slice_to_bytes_mut<T: Dtype>(slice: &mut [T]) -> &mut [u8] {
  let len = slice.len();
  let byte_len = len * (T::PHYSICAL_BITS / 8);
  &mut *std::ptr::slice_from_raw_parts_mut(
    mem::transmute::<_, *mut u8>(slice.as_ptr()),
    byte_len,
  )
}
