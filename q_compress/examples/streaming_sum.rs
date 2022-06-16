use std::io::Write;
use std::time::Instant;

use futures::{StreamExt, TryStreamExt};
use rand::Rng;

use q_compress::{DecompressedItem, Decompressor};
use q_compress::errors::QCompressResult;

async fn streaming_sum_reduce(
  state: (Decompressor<i32>, i32),
  compressed_blob: &[u8],
) -> QCompressResult<(Decompressor<i32>, i32)> {
  let (mut decompressor, mut sum) = state;
  decompressor.write_all(compressed_blob).unwrap();
  for maybe_item in &mut decompressor {
    let item = maybe_item?;
    if let DecompressedItem::Numbers(nums) = item {
      for n in nums {
        sum += n;
      }
    };
  }
  // Once you have decoded as much as possible from the compressed bytes,
  // it should be performant to free the memory used by those compressed bytes.
  decompressor.free_compressed_memory();
  Ok((decompressor, sum))
}

#[tokio::main]
async fn main() -> QCompressResult<()> {
  let mut rng = rand::thread_rng();
  let mut nums = Vec::<i32>::new();
  for _ in 0..1000000 {
    nums.push(rng.gen_range(0..1000));
  }
  let compressed_bytes = q_compress::auto_compress(&nums, 6);
  let compressed_blobs = compressed_bytes.chunks(10000);

  let input_stream = futures::stream::iter(compressed_blobs);
  let start_t = Instant::now();
  let (_, sum) = input_stream
    .map(Ok)
    .try_fold(
      (Decompressor::<i32>::default(), 0_i32),
      streaming_sum_reduce,
    )
    .await?;
  println!("summed to {} in {:?}", sum, Instant::now() - start_t);
  Ok(())
}