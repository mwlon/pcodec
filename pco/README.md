[![Crates.io][crates-badge]][crates-url]

[crates-badge]: https://img.shields.io/crates/v/pco.svg

[crates-url]: https://crates.io/crates/pco

<!---TODO: remove the following stuff in the next release now that it's in the doc comments-->

# Quick Start

```rust
use pco::standalone::{simpler_compress, simple_decompress};
use pco::DEFAULT_COMPRESSION_LEVEL;
use pco::errors::PcoResult;

fn main() -> PcoResult<()> {
  // your data
  let mut my_nums = Vec::new();
  for i in 0..100000 {
    my_nums.push(i as i64);
  }

  // compress
  let compressed: Vec<u8> = simpler_compress(&my_nums, DEFAULT_COMPRESSION_LEVEL)?;
  println!("compressed down to {} bytes", compressed.len());

  // decompress
  let recovered = simple_decompress::<i64>(&compressed)?;
  println!("got back {} ints from {} to {}", recovered.len(), recovered[0], recovered.last().unwrap());
  Ok(())
}
```

For information about Pco in general, see [the main README](../README.md).

For documentation, [docs.rs has the best examples and API details](https://docs.rs/pco/).

# API Notes

* In some places, Pco methods accept a destination (either W: Write or &mut [T: NumberLike]).
  If Pco returns an error, it is possible both the destination and the struct
  have been modified.
* Pco will always try to process all numbers, and it will fail if insufficient bytes are
  available. For instance, during decompression Pco will try to fill the entire `&mut [T]`
  passed in, returning an insufficient data error if the `&[u8]` passed in is not long enough.
