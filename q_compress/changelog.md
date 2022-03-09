# `q_compress` Changelog

## 0.9.0

* Improved decompression speed (20-25% in interesting cases, up to 50% in
sparse case).
* `BitReader` changes: now reads from `&[usize]` instead of `&[u8]`,
necessitating a new wrapper type `BitWords` containing both a `Vec<usize>` and
information about the total number of bits. `.read_aligned_bytes()` now returns
a `Vec<u8>` instead of a slice.
* `UnsignedLike` changes: no longer requires `From<u8>`, now requires
`from_word(word: usize) -> Self` instead.

## 0.8.0

* Improved compression speed in most cases (up to 40%).
* Removed need to implement `num_cmp` and `num_eq` for `NumberLike`.
* Renamed `BitWriter.pop()` to `.bytes()` and made it no longer destructive.
* Added new `NumberLike` implementations: `i16`, `u16`, `u128`.

## 0.7.0

* Changed `BitReader` and `Decompressor::simple_decompress` to accept `&[u8]`
instead of `Vec<u8>`.
* Added stateful `ChunkBodyDecompressor` to enable decompressing a specific
batch size of numbers at a time, giving more fine-grained control for
constrained-memory use cases and taking just the first few numbers.
* Improved compression speed 10% by making `BitWriter` maintain a `Vec<usize>`
instead of `Vec<u8>` and making the compressor write Huffman codes more
efficiently. In doing so, changed the `UnsignedLike` trait to require
`lshift_word` and `rshift_word` implementations instead of `last_u8`.
* Standardized naming to use `_idx` instead of `_ind`.

## 0.6.1

* Made compression of interesting distributions 50%-300% faster by improving
prefix search algorithm.
* Made compression of constant data several times faster.
* Overwrite prefix optimization algorithm with a provably optimal one,
reducing most file sizes ~0.05% and making compression of high compression
levels ~100% faster. Inadvertently improved decompression speed of certain
distributions by ~10% via better choice of prefixes.
* Added timestamp validation functionality.

## 0.6.0

* Added support for delta encoding, which can compress correlated data
to a small fraction of the size in cases with correlated data.
* Eliminated all known panic cases.
Notably, an error is now returned on decompressing to the end of a `BitReader`,
instead of a panic.
* Changed `.simple_compress` to return `Vec<T>` instead of `Result<Vec<T>>`
because all error cases are unreachable.
* Trimmed unnecessary functionality from the public API.
* Renamed `types` module to `data_types` and made public exports for timestamp
types go through it.

## 0.5.0 (yanked)

* Simplified error handling to 3 error kinds: invalid argument, corruption,
and version incompatibility.
* Changed Huffman decoding approach for a decompression speed improvement of
50% in most cases.
* Changed naming of `CompressorConfig`'s `max_depth` to `compression_level`
* Fixed bug where in certain cases with high compression level and spiky
distributions, compressed metadata would be wrong.
* Yanked from crates.io because it introduced a backward incompatibility in
chunk metadata.
Version 0.6 added backward compatibility tests that will prevent this in
the future.

## 0.4.1

* Fixed a mistake in `BoolCompressor` and `BoolDecompressor` typing.
* Changed invalid Huffman tree data corruptions to error instead of panic.

## 0.4.0

* Established stability of file format; for versions `a` and `b` satisfying
`0.4.0 <= a <= b`, an application running version `b` will be able to
decompress a file written by version `a`.
* Implemented chunking.
