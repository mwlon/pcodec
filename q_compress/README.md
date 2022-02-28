[![Crates.io][crates-badge]][crates-url]

[crates-badge]: https://img.shields.io/crates/v/q-compress.svg
[crates-url]: https://crates.io/crates/q-compress

# Quantile Compression

## Usage

```rust
use q_compress::{Compressor, Decompressor};

fn main() {
  // your data
  let mut my_ints = Vec::new();
  for i in 0..100000 {
    my_ints.push(i as i64);
  }
 
  // Here we just use the default configuration, but we can also use
  // `::from_compressor_config()` to set configurations like
  // `compression_level` and `delta_encoding_order`.
  let compressor = Compressor::<i64>::default();
  let bytes: Vec<u8> = compressor.simple_compress(&my_ints);
  println!("compressed down to {} bytes", bytes.len());
 
  // decompress
  let decompressor = Decompressor::<i64>::default();
  let recovered = decompressor.simple_decompress(&bytes).expect("failed to decompress");
  println!("got back {} ints from {} to {}", recovered.len(), recovered[0], recovered.last().unwrap());
}
```

To run something right away, see
[the primary example](./examples/primary.md).

For a lower-level API that allows writing/reading one chunk at a time and
extracting all metadata, see [the docs.rs documentation](https://docs.rs/q_compress/latest/q_compress/).

## Library Changelog

See [changelog.md](./changelog.md)

## Advanced

### Custom Data Types

Small data types can be efficiently compressed in expansion:
for example, compressing `u8` data as a sequence of `u16`
values.  The only cost to using a larger datatype is a small
increase in chunk metadata size.

When necessary, you can implement your own data type via
`q_compress::types::NumberLike` and (if the existing signed/unsigned
implementations are insufficient)
`q_compress::types::SignedLike` and
`q_compress::types::UnsignedLike`.

### Seeking and Quantile Statistics

Recall that each chunk has a metadata section containing
* the total count of numbers in the chunk,
* the ranges for the chunk and count of numbers in each range,
* and the size in bytes of the compressed body.

Using the compressed body size, it is easy to seek through the whole file
and collect a list of all the chunk metadatas.
One can aggregate them to obtain the total count of numbers in the whole file
and even an approximate histogram.
This is typically about 100x faster than decompressing all the numbers.

See the [fast seeking example](./examples/fast_seeking.rs).
