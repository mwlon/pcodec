# `q_compress` Changelog

## 0.4.0

* Established stability of file format; for versions `a` and `b` satisfying
`0.4.0 <= a <= b`, an application running version `b` will be able to
decompress a file written by version `a`.
* Implemented chunking

## 0.4.1

* Fixed a mistake in `BoolCompressor` and `BoolDecompressor` typing
* Changed invalid Huffman tree data corruptions to error instead of panic

## 0.5.0

* Simplified error handling to 3 error kinds: invalid argument, corruption,
and version incompatibility
* Changed Huffman decoding approach for a decompression speed improvement of
50% in most cases.
* Changed naming of `CompressorConfig`'s `max_depth` to `compression_level`
* Fixed bug where in certain cases with high compression level and spiky
distributions, compressed metadata would be wrong.