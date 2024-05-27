#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

#[cfg(test)]
mod tests {
  use crate::*;

  #[test]
  fn test_invertible() {
    let original = [1, 2, 3, 4, 5, 6, 7, 8];
    let mut for_compression = original.clone();
    let level = 3;
    let mut compressed = vec![0_u8; 30];
    let mut decompressed = vec![0; 8];
    unsafe {
      let csize = spdp_compress_batch(
        level,
        for_compression.len(),
        for_compression.as_mut_ptr(),
        compressed.as_mut_ptr(),
      );
      spdp_decompress_batch(
        level,
        csize,
        compressed.as_mut_ptr(),
        decompressed.as_mut_ptr(),
      );
    }
    assert_eq!(decompressed, original);
  }
}
