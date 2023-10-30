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
  let bytes: Vec<u8> = auto_compress(&my_ints, DEFAULT_COMPRESSION_LEVEL);
  println!("compressed down to {} bytes", bytes.len());
 
  // decompress
  let recovered = auto_decompress::<i64>(&bytes).expect("failed to decompress");
  println!("got back {} ints from {} to {}", recovered.len(), recovered[0], recovered.last().unwrap());
}
```

To run something right away, try
[the benchmarks](../bench/README.md).

For a lower-level standalone API that allows writing one chunk at a time /
batched reads, see [the docs.rs documentation](https://docs.rs/pco/latest/pco/).

## Usage as a Wrapped Format

To embed/interleave `pco` in another data format, it is better to use
the [wrapped API and format](src/wrapped) than standalone. 
This allows
* fine-level data paging with good compression ratio down to page sizes of >20 numbers
(as long as the overall chunk has >2k or so)
* less bloat by omitting metadata that the wrapping format must retain

## Important API Note

In some places, pco methods accept a destination (either W: Write or &mut [T: NumberLike]).
If pco returns an error, the pco struct's state should be unaffected, but the destination
may have been modified.

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
