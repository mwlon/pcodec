use std::io::Write;
use crate::{Compressor, Decompressor};
use crate::errors::ErrorKind;

#[test]
fn test_errors_do_not_mutate_decompressor() {
  let nums = vec![1, 2, 3, 4, 5];
  let compressor = Compressor::default();
  let compressed = compressor.simple_compress(&nums);
  let mut decompressor = Decompressor::<i32>::default();

  // header shouldn't leave us in a dirty state
  let mut i = 0;
  while i < compressed.len() + 1 {
    match decompressor.header() {
      Ok(_) => break,
      Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => (),
      Err(e) => panic!("{}", e),
    };
    decompressor.write_all(&compressed[i..i + 1]).unwrap();
    i += 1;
  }

  // chunk metadata shouldn't leave us in a dirty state
  while i < compressed.len() + 1 {
    match decompressor.chunk_metadata() {
      Ok(_) => break,
      Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => (),
      Err(e) => panic!("{}", e),
    };
    decompressor.write_all(&compressed[i..i + 1]).unwrap();
    i += 1;
  }

  // reading the chunk shouldn't leave us in a dirty state
  let mut rec_nums = Vec::new();
  while i < compressed.len() + 1 {
    println!("ITER {}", i);
    match decompressor.chunk_body() {
      Ok(x) => {
        rec_nums.extend(x);
        break
      },
      Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => (),
      Err(e) => panic!("{}", e),
    };
    decompressor.write_all(&compressed[i..i + 1]).unwrap();
    i += 1;
  }

  assert_eq!(rec_nums, nums);
}