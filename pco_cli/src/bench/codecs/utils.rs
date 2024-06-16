use std::mem;

use crate::dtypes::PcoNumberLike;

// cursed ways to convert nums to bytes and back without doing work
pub unsafe fn num_slice_to_bytes<T: PcoNumberLike>(slice: &[T]) -> &[u8] {
  let byte_len = mem::size_of_val(slice);
  &*std::ptr::slice_from_raw_parts(
    mem::transmute::<*const T, *const u8>(slice.as_ptr()),
    byte_len,
  )
}

pub unsafe fn num_slice_to_bytes_mut<T: PcoNumberLike>(slice: &mut [T]) -> &mut [u8] {
  let byte_len = mem::size_of_val(slice);
  &mut *std::ptr::slice_from_raw_parts_mut(
    mem::transmute::<*mut T, *mut u8>(slice.as_mut_ptr()),
    byte_len,
  )
}
