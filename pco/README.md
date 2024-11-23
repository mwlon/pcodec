<div style="text-align:center">
  <img alt="Pco logo: a pico-scale, compressed version of the Pyramid of Khafre in the palm of your hand" src="https://raw.githubusercontent.com/mwlon/pcodec/cac902e714077426d915f4fc397508b187c72380/images/logo.svg" width="160px">
</div>

Pco (Pcodec) losslessly compresses and decompresses numerical sequences with
high compression ratio and moderately fast speed.

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

# Compilation Notes

**For best performance on x86_64, compile with any `bmi*` and `avx*` instruction sets your hardware supports.**
Almost all x86_64 hardware these days supports `bmi1`, `bmi2`, and `avx2`.
This improves compression speed slightly and decompression speed substantially!
To make sure you're using these, you can:

* Add the following to your `~/.cargo/config.toml`:
```toml
[target.'cfg(target_arch = "x86_64")']
rustflags = ["-C", "target-feature=+bmi1,+bmi2,+avx2"]
```
* OR compile with `RUSTFLAGS="-C target-feature=+bmi1,+bmi2,+avx2" cargo build --release ...`

Note that settings `target-cpu=native` does not always have the same effect,
since LLVM compiles for the lowest common denominator of instructions for a
broad CPU family.
