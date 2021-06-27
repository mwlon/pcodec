All figured reported here are calculated using a single thread on a
2.8GHz i5 CPU, operating on in-memory data.
Benchmarks were done by averaging 5 runs on a dataset of 1M numbers
with `max_depth` 6.

Speeds are reported in count of numbers compressed or decompressed
per second with 2 significant figures.

| dataset | compression speed / (million/s) | decompression speed / (million/s) | compression ratio |
--- | --- | --- | ---
| `i64` uniform (incompressible) | 2.7 | 19 | 1.00 |
| `i64` heavy-tail integers | 5.2 | 20 | 4.50 |
| `i64` constant | 40 | 130 | 286,000 |
| `f64` standard normal | 2.5 | 20 | 1.15 |
