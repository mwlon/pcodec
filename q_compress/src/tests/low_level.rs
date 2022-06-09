use std::io::Write;
use crate::{Compressor, CompressorConfig, DecompressedItem, Decompressor};
use crate::data_types::NumberLike;
use crate::decompressor::DecompressorConfig;
use crate::errors::ErrorKind;

#[test]
fn test_low_level_short() {
  let nums = vec![
    vec![0],
    vec![10, 11],
    vec![20, 21, 22],
  ];
  assert_lowest_level_behavior(nums);
}

#[test]
fn test_low_level_long() {
  let nums = vec![(0..100).collect::<Vec<_>>()];
  assert_lowest_level_behavior(nums);
}

#[test]
fn test_low_level_sparse() {
  let mut nums = vec![false; 1000];
  nums.push(true);
  nums.resize(2000, false);
  assert_lowest_level_behavior(vec![nums]);
}

fn assert_lowest_level_behavior<T: NumberLike>(numss: Vec<Vec<T>>) {
  for delta_encoding_order in [0, 7] {
    println!("deo={}", delta_encoding_order);
    let mut compressor = Compressor::<T>::from_config(
      CompressorConfig::default().with_delta_encoding_order(delta_encoding_order)
    );
    compressor.header().unwrap();
    let mut metadatas = Vec::new();
    for nums in &numss {
      metadatas.push(compressor.chunk(nums).unwrap());
    }
    compressor.footer().unwrap();

    let bytes = compressor.drain_bytes();

    let mut decompressor = Decompressor::<T>::from_config(
      DecompressorConfig::default().with_numbers_limit_per_item(2)
    );
    decompressor.write_all(&bytes).unwrap();
    let flags = decompressor.header().unwrap();
    assert_eq!(&flags, compressor.flags());
    let mut chunk_idx = 0;
    let mut chunk_nums = Vec::<T>::new();
    for maybe_item in &mut decompressor {
      let item = maybe_item.unwrap();
      match item {
        DecompressedItem::Flags(_) => panic!("already read flags"),
        DecompressedItem::ChunkMetadata(meta) => {
          assert_eq!(&meta, &metadatas[chunk_idx]);
          if chunk_idx > 0 {
            assert_eq!(&chunk_nums, &numss[chunk_idx - 1]);
            chunk_nums = Vec::new();
          }
          chunk_idx += 1;
        },
        DecompressedItem::Numbers(nums) => {
          chunk_nums.extend(&nums);
        }
      }
    }
    assert_eq!(&chunk_nums, numss.last().unwrap());

    let terminated_err = decompressor.chunk_metadata().unwrap_err();
    assert!(matches!(terminated_err.kind, ErrorKind::InvalidArgument));
  }
}