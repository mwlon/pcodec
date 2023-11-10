# `pco`

**⚠️ Both the API and the data format are unstable for the 0.0.0-alpha.\*
releases. Do not depend on pco for long-term storage yet. ⚠️**

## Quick Start

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
  let compressed: Vec<u8> = auto_compress(&my_ints, DEFAULT_COMPRESSION_LEVEL);
  println!("compressed down to {} bytes", compressed.len());

  // decompress
  let recovered = auto_decompress::<i64>(&compressed).expect("failed to decompress");
  println!("got back {} ints from {} to {}", recovered.len(), recovered[0], recovered.last().unwrap());
}
```

To run something right away, try
[the benchmarks](../bench/README.md).

## Standalone vs Wrapped

Pco can be used as a standalone format (as in the above example).
Good reasons to do so:
* a quick proof of concept for the compression ratio or performance
* all you need to do is transmit a long list of numbers

However, the standalone format is quite limited, so it is expected that most
use cases will wrap pco in a different format (imagine, say, Parquet).
This could unlock things like
* nullability
* fast seeking through the file
* fast filtering
* a schema or key:value metadata

In either case,
[docs.rs has more examples and API details](https://docs.rs/pco/).

## Important API Notes

* In some places, pco methods accept a destination (either W: Write or &mut [T: NumberLike]).
If pco returns an error, it is possible both the destination and the struct
have been modified.
* Pco will always try to process all numbers, and it will fail if insufficient bytes are
available. For instance, during decompression pco will try to fill the entire `&mut [T]`
passed in, returning an insufficient data error if the `&[u8]` passed in is not long enough.

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

The maximum legal precision of a custom data type is currently 128 bits.
