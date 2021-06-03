# Quantile Compression

This rust library compresses and decompresses sequences of
numerical data very well.
It currently only supports the `i64` data type.
For natural data, it typically compresses down to files 25-40% smaller than
ones produced by `gzip -9`, and decompresses several times faster.

This IS:
* lossless
* order-preserving
* moderately fast

This is NOT:
* lossy
* order-agnostic / compression for multisets
* competing for decompression speed

To get started, look at `example/`.