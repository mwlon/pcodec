use std::io::Write;

use futures::{StreamExt, TryStreamExt};
use rand::Rng;

use crate::{DecompressedItem, Decompressor, DEFAULT_COMPRESSION_LEVEL};
use crate::errors::QCompressResult;

#[derive(Default)]
struct State {
  decompressor: Decompressor<i32>,
  nums: Vec<i32>,
}

async fn streaming_collect(
  state: State,
  compressed_blob: &[u8],
) -> QCompressResult<State> {
  let State { mut decompressor, mut nums } = state;
  decompressor.write_all(compressed_blob).unwrap();
  for maybe_item in &mut decompressor {
    let item = maybe_item?;
    if let DecompressedItem::Numbers(batch) = item {
      nums.extend(batch);
    } else {
      println!("{:?}", item);
    }
  }
  // Once you have decoded as much as possible from the compressed bytes,
  // it should be performant to free the memory used by those compressed bytes.
  decompressor.free_compressed_memory();
  Ok(State { decompressor, nums })
}

#[tokio::test]
async fn test_streaming_decompress() -> QCompressResult<()> {
  let mut rng = rand::thread_rng();
  let mut true_nums = Vec::<i32>::new();
  let n: usize = 100000;
  for _ in 0..n {
    true_nums.push(rng.gen_range(0..1000));
  }
  let compressed_bytes = crate::auto_compress(&true_nums, DEFAULT_COMPRESSION_LEVEL);
  let compressed_blobs = compressed_bytes.chunks(10000);

  let input_stream = futures::stream::iter(compressed_blobs);
  let State {nums, ..} = input_stream
    .map(Ok)
    .try_fold(
      State::default(),
      streaming_collect,
    )
    .await?;
  for i in 0..n {
    assert_eq!(nums[i], true_nums[i], "{}", i)
  }
  Ok(())
}