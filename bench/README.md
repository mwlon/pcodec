# Benchmarks

This generates a wide variety of common distributions,
compresses them, decompresses them, and makes sure
all the data came back bitwise identical.
It supports
* multiple codecs (pco, q\_compress, zstd)
* multiple data types

## Running

TL;DR (`cd`'d into the repo):
* `python bench/generate_randoms.py`
* `cargo run --release --bin bench`

The script to generate the data uses python, so set up a python3
environment with `numpy` and `pyarrow` installed.
In that environment, run
`python bench/generate_randoms.py`.
This will populate some human-readable data in `bench/data/txt/` and
the exact same numerical data as bytes in `bench/data/binary/`.
For instance,
```
% head -5 bench/data/txt/f64_normal_at_0.txt
1.764052345967664
0.4001572083672233
0.9787379841057392
2.240893199201458
1.8675579901499675
```
shows floats sampled from a standard normal distribution.

Then to run pco and decompression on each dataset, run
`cargo run --release --bin bench`.
This will show the compressed size and how long
it took to compress and decompress each dataset.
You can see the compressed files in
`bench/data/pco/`.

Check `cargo run --release --bin bench -- --help` for information on how to
run other codecs, configure codecs differently, only run specific datasets,
etc.

## Results

All figures reported here are calculated using a single thread on an Apple
M3 performance core, operating on in-memory data, using Rust 1.73.
Benchmarks were done by taking the median of 100 runs on a dataset of 1M
numbers with `compression_level` 8.

Speeds are reported in count of numbers compressed or decompressed
per second with 2 significant figures.
Compression ratio is reported with 3 significant figures.

| dataset            | compression speed / (million/s) | decompression speed / (million/s) | compression ratio |
|--------------------|---------------------------------|-----------------------------------|-------------------|
| `f64_decimal`      | 30                              | 230                               | 4.67              |
| `f64_slow_cosine`  | 39                              | 260                               | 4.35              |
| `i64_lomax05_reg`  | 43                              | 490                               | 4.62              |
| `i64_sparse`       | 100                             | 520                               | 792               |
| `micros_millis`    | 32                              | 440                               | 2.08              |

`i64` and `f64` are each 8 bytes, so compression is around 250-350MB/s,
and decompression is around 2-4GB/s.
For reference, on the same hardware and `i64_lomax05_reg` dataset, ZStandard
`0.12.3+zstd.1.5.2` gets:

* level 3: compresses 40 million/s, decompresses 110 million/s, compression
  ratio 3.14.
* level 22: compresses 0.44 million/s, decompresses 170 million/s,
  compression ratio 3.51.
