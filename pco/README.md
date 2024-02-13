[![Crates.io][crates-badge]][crates-url]

[crates-badge]: https://img.shields.io/crates/v/pco.svg
[crates-url]: https://crates.io/crates/pco

# Quick Start

```rust
use pco::standalone::{auto_compress, auto_decompress};
use pco::DEFAULT_COMPRESSION_LEVEL;

fn main() {
  // your data
  let mut my_ints = Vec::new();
  for i in 0..100000 {
    my_ints.push(i as i64);
  }

  // compress
  let compressed: Vec<u8> = auto_compress(&my_ints, DEFAULT_COMPRESSION_LEVEL);
  println!("compressed down to {} bytes", compressed.len());

  // decompress
  let recovered = auto_decompress::<i64>(&compressed).expect("failed to decompress");
  println!("got back {} ints from {} to {}", recovered.len(), recovered[0], recovered.last().unwrap());
}
```

To run something right away, try
[the benchmarks](../bench/README.md).

For information about Pco in general, see [the main README](../README.md).

For documentation, [docs.rs has the best examples and API details](https://docs.rs/pco/).

# API Notes

* In some places, Pco methods accept a destination (either W: Write or &mut [T: NumberLike]).
If Pco returns an error, it is possible both the destination and the struct
have been modified.
* Pco will always try to process all numbers, and it will fail if insufficient bytes are
available. For instance, during decompression Pco will try to fill the entire `&mut [T]`
passed in, returning an insufficient data error if the `&[u8]` passed in is not long enough.

# Advanced

## Custom Data Types

Small data types can be efficiently compressed in expansion:
for example, compressing `u16` data as a sequence of `u32`
values.  The only cost to using a larger datatype is a very small
increase in chunk metadata size.

When necessary, you can implement your own data type via
`pco::data_types::NumberLike` and (if the existing
implementations are insufficient)
`pco::data_types::UnsignedLike` and
`pco::data_types::FloatLike`.

The maximum legal precision of a custom data type is currently 128 bits.
