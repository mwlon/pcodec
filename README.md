# Quantile Compression

This rust library compresses and decompresses sequences of
numerical data very well.
It currently supports the following data types:
`i32`, `i64`, `u32`, `u64`, `f32`, `f64`.
Timestamp support may come soon in the future.

For natural data, it typically compresses down to files 25-40% smaller than
ones produced by `gzip -9`, and decompresses several times faster.

This IS:
* lossless
* order-preserving
* moderately fast

This is NOT:
* lossy
* for multisets
* optimal for time series with high mutual information between consecutive elements
* competing for decompression speed

## Usage

See the following basic usage.
To run something right away, see [the example](./example/example.md).

```
use q_compress:{BitReader, I64Compressor, I64Decompressor};

fn main() {
  // your data
  let mut my_ints = Vec::new();
  for i in 0..100000 {
    my_ints.push(i as i64);
  }
  
  // compress
  let max_depth = 6; // basically compression level - 6 is generally good
  let compressor = I64Compressor::train(&my_ints, max_depth).expect("failed to train");
  let bytes = compressor.compress(&my_ints).expect("out of range");
  println!("compressed down to {} bytes", bytes.len());
  
  // decompress
  let bit_reader = &mut BitReader::from(bytes);
  let decompressor = I64Decompressor::from_reader(bit_reader).expect("couldn't read compression scheme");
  let recovered = decompressor.decompress(bit_reader);
  println!("got back {} ints from {} to {}", recovered.len(), recovered[0], recovered.last().unwrap());
}
```

## Method

This works by describing each number with a _range_ and an _offset_.
The range specifies an inclusive range `[lower, upper]` that the
number might be in, and the offset specifies the exact position within that
range.
The compressor chooses a _prefix_ for each range via Huffman
codes.

For data sampled from a random distribution, this compression algorithm can
reduce byte size to near the theoretical limit of the distribution's Shannon
entropy.
