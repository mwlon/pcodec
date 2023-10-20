use std::io::Write;

use crate::errors::ErrorKind;
use crate::standalone::{FileDecompressor, simple_compress};
use crate::standalone;
use crate::compressor_config::CompressorConfig;

#[test]
fn test_errors_do_not_mutate_decompressor() {
  let nums = vec![1, 2, 3, 4, 5];
  let compressed = simple_compress(&nums, CompressorConfig::default()).unwrap();
  let mut data = &compressed[..];
  let mut file_decompressor = None;

  // header shouldn't leave us in a dirty state
  for i in 0..data.len() + 1 {
    match FileDecompressor::new(&data[..i]) {
      Ok((fd, rest)) => {
        file_decompressor = Some(fd);
        data = rest;
        break;
      },
      Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => (),
      Err(e) => panic!("{}", e),
    }
  }
  let file_decompressor = file_decompressor.unwrap();

  // chunk metadata shouldn't leave us in a dirty state
  let mut chunk_decompressor = None;
  for i in 0..data.len() + 1 {
    match file_decompressor.chunk_decompressor::<i32>(&data[..i]) {
      Ok((cd, rest)) => {
        chunk_decompressor = Some(cd.unwrap());
        data = rest;
        break;
      },
      Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => (),
      Err(e) => panic!("{}", e),
    };
  }
  let mut chunk_decompressor = chunk_decompressor.unwrap();

  // reading the chunk shouldn't leave us in a dirty state
  let mut rec_nums = Vec::new();
  for i in 0..data.len() + 1 {
    match chunk_decompressor.decompress_remaining_extend(&data[..i], &mut rec_nums) {
      Ok(rest) => {
        data = rest;
        break;
      }
      Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => (),
      Err(e) => panic!("{}", e),
    };
  }

  assert_eq!(rec_nums, nums);
}
