All figured reported here are calculated using a single thread on a
2.8GHz i5 CPU, operating on in-memory data, using Rust 1.70.
Benchmarks were done by averaging 100 runs on a dataset of 1M numbers
with `compression_level` 8.

Speeds are reported in count of numbers compressed or decompressed
per second with 2 significant figures.
Compression ratio is reported with 3 significant figures.

| dataset            | compression speed / (million/s) | decompression speed / (million/s) | compression ratio |
|--------------------|---------------------------------|-----------------------------------|-------------------|
| `f64_decimal`      | 10                              | 63                                | 4.67              |
| `f64_slow_cosine`  | 13                              | 91                                | 4.35              |
| `i64_lomax05_long` | 14                              | 140                               | 4.62              |
| `i64_sparse`       | 36                              | 220                               | 792               |
| `micros_millis`    | 11                              | 120                               | 2.08              |

`i64` and `f64` are each 8 bytes, so these speeds are in the ballpark of 1GB/s.
For reference, on the same hardware and heavy-tail integers dataset, ZStandard
`0.12.3+zstd.1.5.2` gets:

* level 3: compresses 13 million/s, decompresses 52 million/s, compression
ratio 3.14.
* level 22: compresses 0.15 million/s, decompresses 48 million/s,
compression ratio 3.51.
