# `pco`

## Usage as a Standalone Format

```rust
use pco::standalone::{auto_compress, auto_decompress};
use pco::DEFAULT_COMPRESSION_LEVEL;

fn main() {
  // your data
  let mut my_ints = Vec::new();
  for i in 0..100000 {
    my_ints.push(i as i64);
  }
 
  // Here we let the library choose a configuration with default compression
  // level. If you know about the data you're compressing, you can compress
  // faster by creating a `CompressorConfig`.
  let bytes: Vec<u8> = auto_compress(&my_ints, DEFAULT_COMPRESSION_LEVEL);
  println!("compressed down to {} bytes", bytes.len());
 
  // decompress
  let recovered = auto_decompress::<i64>(&bytes).expect("failed to decompress");
  println!("got back {} ints from {} to {}", recovered.len(), recovered[0], recovered.last().unwrap());
}
```

To run something right away, try
[the benchmarks](../bench/README.md).

For a lower-level standalone API that allows writing/reading one chunk at a time and
extracting all metadata, see [the docs.rs documentation](https://docs.rs/pco/latest/pco/).

## Usage as a Wrapped Format

To embed/interleave `pco` in another data format, it is better to use
the [wrapped API and format](src/wrapped) than standalone. 
This allows
* fine-level data paging with good compression ratio down to page sizes of >20 numbers
(as long as the overall chunk has >2k or so)
* less bloat by omitting metadata that the wrapping format must retain

## Advanced

### Custom Data Types

Small data types can be efficiently compressed in expansion:
for example, compressing `u16` data as a sequence of `u32`
values.  The only cost to using a larger datatype is a very small
increase in chunk metadata size.

When necessary, you can implement your own data type via
`pco::data_types::NumberLike` and (if the existing
implementations are insufficient)
`pco::data_types::UnsignedLike` and
`pco::data_types::FloatLike`.

### Seeking and Statistics

Each chunk has a metadata section containing
* the total count of numbers in the chunk,
* the ranges for the chunk and count of numbers in each range,
* and the size in bytes of the compressed body.

Using the compressed body size, it is easy to seek through the whole file
and collect a list of all the chunk metadatas.
One can aggregate them to obtain the total count of numbers in the whole file
and even an approximate histogram.
This is typically about 100x faster than decompressing all the numbers.
