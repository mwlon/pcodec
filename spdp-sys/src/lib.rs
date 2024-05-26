extern "C" {
  pub fn spdp_compress_batch(level: u8, length: usize, src: *mut u8, dst: *mut u8) -> usize;
  pub fn spdp_decompress_batch(level: u8, length: usize, src: *mut u8, dst: *mut u8);
}

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
