# Quantile Compression

This rust library compresses and decompresses sequences of
numerical data very well.
It currently only supports the `i64` data type.
For natural data, it typically compresses down to files 25-40% smaller than
ones produced by `gzip -9`, and decompresses several times faster.

This IS:
* lossless
* order-preserving
* moderately fast

This is NOT:
* lossy
* order-agnostic / compression for multisets
* competing for decompression speed

# Usage

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
  let bytes = compressor.compress(&my_ints);
  println!("compressed down to {} bytes", bytes.len());
  
  // decompress
  let bit_reader = &mut BitReader::new(bytes);
  let decompressor = I64Decompressor::from_reader(bit_reader);
  let recovered = decompressor.decompress(bit_reader);
  println!("got back {} ints from {} to {}", recovered.len(), recovered[0], recovered.last().unwrap());
}
```