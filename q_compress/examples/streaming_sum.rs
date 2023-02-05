use std::io::Write;
use std::time::Instant;

use futures::{StreamExt, TryStreamExt};
use rand::Rng;

use q_compress::errors::QCompressResult;
use q_compress::{DecompressedItem, Decompressor, DEFAULT_COMPRESSION_LEVEL};

async fn streaming_sum_reduce(
  state: (Decompressor<i32>, i32, usize),
  compressed_blob: &[u8],
) -> QCompressResult<(Decompressor<i32>, i32, usize)> {
  let (mut decompressor, mut sum, mut count) = state;
  decompressor.write_all(compressed_blob).unwrap();
  for maybe_item in &mut decompressor {
    let item = maybe_item?;
    if let DecompressedItem::Numbers(nums) = item {
      for n in nums {
        sum += n;
        count += 1;
      }
    }
  }
  // Once you have decoded as much as possible from the compressed bytes,
  // it should be performant to free the memory used by those compressed bytes.
  decompressor.free_compressed_memory();
  Ok((decompressor, sum, count))
}

#[tokio::main]
async fn main() -> QCompressResult<()> {
  let mut rng = rand::thread_rng();
  let mut nums = Vec::<i32>::new();
  let n: usize = 1000000;
  for _ in 0..n {
    nums.push(rng.gen_range(0..1000));
  }
  let true_sum = nums.iter().sum();
  let compressed_bytes = q_compress::auto_compress(&nums, DEFAULT_COMPRESSION_LEVEL);
  let compressed_blobs = compressed_bytes.chunks(10000);

  let input_stream = futures::stream::iter(compressed_blobs);
  let start_t = Instant::now();
  let (_, sum, count) = input_stream
    .map(Ok)
    .try_fold(
      (
        Decompressor::<i32>::default(),
        0_i32,
        0_usize,
      ),
      streaming_sum_reduce,
    )
    .await?;
  assert_eq!(count, n);
  assert_eq!(sum, true_sum);
  println!("summed in {:?}", Instant::now() - start_t);
  Ok(())
}
