All figured reported here are calculated using a single thread on a
2.8GHz i5 CPU, operating on in-memory data, using Rust 1.61.
Benchmarks were done by averaging 100 runs on a dataset of 1M numbers
with `compression_level` 8.

Speeds are reported in count of numbers compressed or decompressed
per second with 2 significant figures.
Compression ratio is reported with 3 significant figures.
For the `i64` heavy-tail integers, a lomax distribution with alpha parameter 0.5 and median 1000 was used.

| dataset                        | compression speed / (million/s) | decompression speed / (million/s) | compression ratio |
|--------------------------------|---------------------------------|-----------------------------------|-------------------|
| `i64` constant                 | 62                              | 480                               | 216,000           |
| `i64` sparse                   | 77                              | 290                               | 597               |
| `i64` uniform (incompressible) | 14                              | 68                                | 1.00              |
| `i64` heavy-tail integers      | 14                              | 48                                | 4.63              |
| `f64` standard normal          | 11                              | 40                                | 1.15              |
| `f64` slow cosine              | 11                              | 31                                | 4.36              |
| `TimestampMicros` millis       | 11                              | 47                                | 2.14              |

`i64` and `f64` are each 8 bytes, so for the more interesting distributions
(e.g. heavy-tail integers and standard normal),
this is a decompression speed of 300-400MB/s.

For reference, on the same hardware and heavy-tail integers dataset, ZStandard
`0.10.0+zstd.1.5.2` gets:

* level 3: compresses 13 million/s, decompresses 52 million/s, compression
ratio 3.14.
* level 22: compresses 0.15 million/s, decompresses 48 million/s,
compression ratio 3.51.
