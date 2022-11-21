# `q_compress` Changelog

## 0.11.5 (not yet released)
* Increased compression speed 4% in most cases by making `BitWriter` implementation cleverer.

## 0.11.4

* Increased decompression speed another 8-20% for interesting distributions by
making `BitReader` implementation cleverer.
* Fixed an atomicity bug during streaming decompression that could lead to
panics or incorrect results.

## 0.11.3

* Improved decompression speed 8-18% for non-sparse distributions by compiling
sparse vs non-sparse loops separately.

## 0.11.2

* Added support for "wrapped mode" - a way to write/read quantile-compressed
data with finer granularity (data page instead of chunk) and less bloat for
interleaving within another wrapping columnar data format that manages its own
count and compressed body size statistics. Usable via
`::wrapped::{Compressor, Decompressor}`. This includes a new flag for whether
wrapped mode was used.
* Fixed some atomicity bugs when returning errors.
* Improved auto delta encoding order heuristic for nearly-constant data.
* Improved compression speed ~8% by streamlining unoptimized prefix algorithm.

## 0.11.1

* Improved prefix optimization speed (part of compression) by 30%.
* Changed compression levels for small data to use fewer prefixes.

## 0.11.0

* Replaced 96-bit timestamps with 64-bit ones for a 30% increase to compression
and decompression speed.
Moved 96-bit timestamps and 128-bit integers to a crate feature (`timestamps_96`).
* Slightly reduced binary size by limiting generics of internal decompressor
implementation.

## 0.10.2

* Changed default compression level to 8.

## 0.10.1

* Rust 1.60 performance regression fix via inlining.

## 0.10.0

* Added `use_gcds` compressor config and flag to improve compression ratio
in cases where all numbers in some ranges share a nontrivial common divisor.
* Made `CompressorConfig`, `DecompressorConfig`, `Prefix`, `Flags`, and
`ChunkMetadata` more API-stable by adding a phantom private field.
* Removed `BitReader`, `BitWords`, `BitWriter` from the public API in favor of
mutable `Compressor`s and `Decompressor`s. See the docs for new examples.
* Removed `ChunkBodyDecompressor` from the public API in favor of a
`Decompressor` `Iterator` implementation that makes streaming much easier.

## 0.9.3

* Added auto compress/decompress functions that can be used as 1-liners.

## 0.9.2

* Defined behavior for decompression failures: the bit reader remains
unmodified.

## 0.9.1

* Introduced a new always-on flag that reduces file size by a few % in cases
of small data by using the minimal number of bits required to encode prefix
counts.

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
