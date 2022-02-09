# `q_compress` Changelog

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
