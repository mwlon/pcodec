use std::io::Write;

use futures::{StreamExt, TryStreamExt};
use rand::Rng;

use crate::data_types::NumberLike;
use crate::errors::QCompressResult;
use crate::{DecompressedItem, Decompressor, DEFAULT_COMPRESSION_LEVEL};

#[derive(Default)]
struct State<T: NumberLike> {
  decompressor: Decompressor<T>,
  nums: Vec<T>,
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
  check_streaming_recovery(nums, 10000).await
}

#[tokio::test]
async fn test_streaming_decompress_sparse() -> QCompressResult<()> {
  let mut rng = rand::thread_rng();
  let mut nums = Vec::<bool>::new();
  let n: usize = 100000;
  for _ in 0..n {
    nums.push(rng.gen_bool(0.15));
  }
  check_streaming_recovery(nums, 10).await
}

async fn check_streaming_recovery<T: NumberLike>(
  true_nums: Vec<T>,
  blob_size: usize,
) -> QCompressResult<()> {
  let compressed_bytes = crate::auto_compress(&true_nums, DEFAULT_COMPRESSION_LEVEL);
  let compressed_blobs = compressed_bytes.chunks(blob_size);

  let input_stream = futures::stream::iter(compressed_blobs);
  let State { nums, .. } = input_stream
    .map(Ok)
    .try_fold(State::<T>::default(), streaming_collect)
    .await?;
  assert_eq!(nums, true_nums);
  Ok(())
}
