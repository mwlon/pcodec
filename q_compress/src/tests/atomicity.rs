use crate::{BitReader, BitWords, Compressor, Decompressor};
use crate::bits::ceil_div;
use crate::errors::ErrorKind;

#[test]
fn test_errors_do_not_mutate_reader() {
  let nums = vec![1, 2, 3, 4, 5];
  let compressor = Compressor::default();
  let compressed = compressor.simple_compress(&nums);
  let decompressor = Decompressor::<i32>::default();

  // header shouldn't leave us in a dirty state
  let mut maybe_flags = None;
  let mut bit_idx = 0;
  for i in 0..compressed.len() + 1 {
    let bit_words = BitWords::from(&compressed[0..i]);
    let mut reader = BitReader::from(&bit_words);
    reader.seek_to(bit_idx);
    println!("i={}/{}, bit_idx={}", i, compressed.len(), bit_idx);
    match decompressor.header(&mut reader) {
      Ok(x) => {
        println!("ok");
        maybe_flags = Some(x);
      }
      Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => (),
      Err(e) => panic!("{}", e),
    };
    bit_idx = reader.bit_idx();
    if maybe_flags.is_some() {
      break;
    }
  }

  // chunk metadata shouldn't leave us in a dirty state
  let flags = maybe_flags.unwrap();
  let mut maybe_chunk_meta = None;
  let lb = ceil_div(bit_idx, 8);
  for i in lb..compressed.len() + 1 {
    println!("i={}/{}, bit_idx={}", i, compressed.len(), bit_idx);
    let bit_words = BitWords::from(&compressed[0..i]);
    let mut reader = BitReader::from(&bit_words);
    reader.seek_to(bit_idx);
    match decompressor.chunk_metadata(&mut reader, &flags) {
      Ok(x) => {
        println!("ok");
        maybe_chunk_meta = x;
      }
      Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => (),
      Err(e) => panic!("{}", e),
    };
    bit_idx = reader.bit_idx();
    if maybe_chunk_meta.is_some() {
      break;
    }
  }

  // reading the chunk shouldn't leave us in a dirty state
  let chunk_meta = maybe_chunk_meta.unwrap();
  let mut maybe_nums = None;
  let lb = ceil_div(bit_idx, 8);
  for i in lb..compressed.len() + 1 {
    println!("i={}/{}, bit_idx={}", i, compressed.len(), bit_idx);
    let bit_words = BitWords::from(&compressed[0..i]);
    let mut reader = BitReader::from(&bit_words);
    reader.seek_to(bit_idx);
    match decompressor.chunk_body(&mut reader, &flags, &chunk_meta) {
      Ok(x) => {
        println!("ok");
        maybe_nums = Some(x);
      }
      Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => (),
      Err(e) => panic!("{}", e),
    };
    bit_idx = reader.bit_idx();
    if maybe_nums.is_some() {
      break;
    }
  }

  assert_eq!(maybe_nums.unwrap(), nums);
}