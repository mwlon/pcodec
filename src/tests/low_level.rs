use crate::{BitReader, BitWriter, Compressor, CompressorConfig, Decompressor};
use crate::data_types::NumberLike;

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
    let mut writer = BitWriter::default();
    let compressor = Compressor::<T>::from_config(CompressorConfig {
      delta_encoding_order,
      ..Default::default()
    });
    compressor.header(&mut writer).unwrap();
    let mut metadatas = Vec::new();
    for nums in &numss {
      metadatas.push(compressor.chunk(nums, &mut writer).unwrap());
    }
    compressor.footer(&mut writer).unwrap();

    let bytes = writer.pop();
    let mut reader = BitReader::from(&bytes);

    let decompressor = Decompressor::<T>::default();
    let flags = decompressor.header(&mut reader).unwrap();
    assert_eq!(&flags, compressor.flags());
    for i in 0..numss.len() {
      let metadata = decompressor.chunk_metadata(
        &mut reader,
        &flags
      ).unwrap().unwrap();
      assert_eq!(&metadata, &metadatas[i]);

      let nums = &numss[i];
      let mut rec_nums = Vec::<T>::new();
      let mut chunk_body = decompressor.get_chunk_body_decompressor(
        &flags,
        &metadata,
      ).unwrap();
      for j in 0..nums.len() + 2 { // +2 to make sure there's no weird behavior after end
        let batch = chunk_body.decompress_next_batch(
          &mut reader,
          1,
        ).unwrap();
        println!("i {} j {} order {}", i, j, delta_encoding_order);
        assert_eq!(
          batch.len(),
          if j < nums.len() { 1 } else { 0 },
        );
        rec_nums.extend(&batch);
      }
      assert_eq!(&rec_nums, nums);
    }
    assert!(decompressor.chunk_metadata(&mut reader, &flags).unwrap().is_none());
  }
}