Pco (Pcodec) losslessly compresses and decompresses numerical sequences with
high compression ratio and moderately fast speed.

# Compilation Notes

**On x86, compile with `bmi1`, `bmi2`, and any `avx*` instruction sets your hardware supports.**
This improves compression speed slightly and decompression speed substantially!
You can either

* Add the following to your `~/.cargo/config.toml`:
```toml
[target.'cfg(target_arch = "x86_64")']
rustflags = ["-C", "target-feature=+bmi1,+bmi2,+avx2"]
```
* OR compile with `RUSTFLAGS="-C target-feature=+bmi1,bmi2,+avx2" cargo build --release ...`

Note that settings `target-cpu=native` does not always have the same effect,
since LLVM compiles for the lowest common denominator of instructions for a
broad CPU family.
