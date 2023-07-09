// use std::mem;
// use std::ptr::slice_from_raw_parts;
// use crate::NumberLike;
//
// pub fn byte_slice_to_nums<T: NumberLike>(bytes: &[u8]) -> &[T] {
//   let bytes_per_num = T::PHYSICAL_BITS / 8;
//   unsafe {
//     mem::transmute(slice_from_raw_parts(mem::transmute::<_, *const T>(bytes.as_ptr()), bytes.len() / bytes_per_num))
//   }
// }
//
// pub fn num_vec_to_bytes<T: NumberLike>(nums: Vec<T>) -> Vec<u8> {
//   let bytes_per_num = T::PHYSICAL_BITS / 8;
//   let byte_len = nums.len() * bytes_per_num;
//   unsafe {
//     Vec::from_raw_parts(
//       mem::transmute::<_, *mut u8>(nums.as_ptr()),
//       byte_len,
//       byte_len,
//     )
//   }
// }
