All figured reported here are calculated using a single thread on a
2.8GHz i5 CPU, operating on in-memory data.
Benchmarks were done by averaging 5 runs on a dataset of 1M numbers
with `max_depth` 6.

Speeds are reported in count of numbers compressed or decompressed
per second with 2 significant figures.
Compression ratio is reported with 3 significant figures.
For the `i64` heavy-tail integers, a lomax distribution with alpha parameter 0.5 and median 1000 was used.

| dataset | compression speed / (million/s) | decompression speed / (million/s) | compression ratio |
--- | --- | --- | ---
| `i64` constant | 44 | 430 | 216,000 |
| `i64` sparse | 46 | 220 | 597 |
| `i64` uniform (incompressible) | 8.3 | 47 | 1.00 |
| `i64` heavy-tail integers | 9.3 | 34 | 4.50 |
| `f64` standard normal | 6.9 | 26 | 1.15 |

`i64` and `f64` are each 8 bytes, so for the more interesting distributions
(e.g. heavy-tail integers and standard normal),
this is a decompression speed of 200-300MB/s.
