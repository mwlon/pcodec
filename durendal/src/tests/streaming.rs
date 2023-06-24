use std::io::Write;

use futures::{StreamExt, TryStreamExt};
use rand::Rng;

use crate::{DecompressorConfig, DEFAULT_COMPRESSION_LEVEL};
use crate::data_types::NumberLike;
use crate::errors::QCompressResult;
use crate::standalone::{DecompressedItem, Decompressor};

struct State<T: NumberLike> {
  decompressor: Decompressor<T>,
  nums: Vec<T>,
}

// decompress in small batches
impl<T: NumberLike> Default for State<T> {
  fn default() -> Self {
    Self {
      decompressor: Decompressor::from_config(DecompressorConfig {
        numbers_limit_per_item: 100,
        ..Default::default()
      }),
      nums: Vec::new(),
    }
  }
}

async fn streaming_collect<T: NumberLike>(
  state: State<T>,
  compressed_blob: &[u8],
) -> QCompressResult<State<T>> {
  let State {
    mut decompressor,
    mut nums,
  } = state;
  decompressor.write_all(compressed_blob).unwrap();
  for maybe_item in &mut decompressor {
    let item = maybe_item?;
    if let DecompressedItem::Numbers(batch) = item {
      nums.extend(batch);
    }
  }
  // Once you have decoded as much as possible from the compressed bytes,
  // it should be performant to free the memory used by those compressed bytes.
  decompressor.free_compressed_memory();
  Ok(State { decompressor, nums })
}

#[tokio::test]
async fn test_streaming_decompress_dense() -> QCompressResult<()> {
  let mut rng = rand::thread_rng();
  let mut nums = Vec::<i32>::new();
  let n: usize = 100000;
  for _ in 0..n {
    nums.push(rng.gen_range(0..1000));
  }
  check_streaming_recovery(&nums, 10000).await
}

#[tokio::test]
async fn test_streaming_decompress_sparse() -> QCompressResult<()> {
  let mut rng = rand::thread_rng();
  let mut nums = Vec::new();
  for _ in 0..10000 {
    nums.push(0);
  }
  for _ in 0..1500 {
    nums.push(rng.gen_range(0..2));
  }
  check_streaming_recovery(&nums, 10).await?;
  check_streaming_recovery(&nums, 1000).await
}

#[tokio::test]
async fn test_streaming_decompress_float_mult() -> QCompressResult<()> {
  let mut nums = Vec::new();
  for i in 0..100 {
    nums.push((i as f32) * std::f32::consts::TAU);
  }
  check_streaming_recovery(&nums, 10).await?;
  check_streaming_recovery(&nums, 1000).await
}

async fn check_streaming_recovery<T: NumberLike>(
  true_nums: &[T],
  blob_size: usize,
) -> QCompressResult<()> {
  let compressed_bytes = crate::standalone::auto_compress(true_nums, DEFAULT_COMPRESSION_LEVEL);
  let compressed_blobs = compressed_bytes.chunks(blob_size);

  let input_stream = futures::stream::iter(compressed_blobs);
  let State { nums, .. } = input_stream
    .map(Ok)
    .try_fold(State::<T>::default(), streaming_collect)
    .await?;
  assert_eq!(nums.len(), true_nums.len());
  for i in 0..nums.len() {
    assert_eq!(nums[i], true_nums[i], "at {}", i);
  }
  Ok(())
}
