# Benchmarks

The benchmarks support many things!
* different codecs
* all sorts of configurations on those codecs
* multiple datasets
  * synthetic, theoretically understood ones
  * arbitrary Parquet files
* measurement for compressed size, compression time, and decompression time

By default, the benchmarks also assert that the data comes back bitwise
identical to the input.

Check `cargo run --release --bin bench -- --help` for most usage information.

## Synthetic

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

### Results

All figures reported here are calculated using a single thread on an Apple
M3 performance core, operating on in-memory data, using Rust 1.73.
Benchmarks were done by taking the median of 100 runs on a dataset of 1M
numbers with `compression_level` 8.

Speeds are reported in count of numbers compressed or decompressed
per second with 2 significant figures.
Compression ratio is reported with 3 significant figures.

| dataset            | compression speed / (million/s) | decompression speed / (million/s) | compression ratio |
|--------------------|---------------------------------|-----------------------------------|-------------------|
| `f64_decimal`      | 30                              | 250                               | 4.67              |
| `f64_slow_cosine`  | 39                              | 270                               | 4.35              |
| `i64_lomax05_reg`  | 43                              | 590                               | 4.62              |
| `i64_sparse`       | 100                             | 430                               | 792               |
| `micros_millis`    | 32                              | 580                               | 2.08              |

`i64` and `f64` are each 8 bytes, so compression is around 250-350MB/s,
and decompression is around 2-4GB/s.
For reference, on the same hardware and `i64_lomax05_reg` dataset, ZStandard
`0.12.3+zstd.1.5.2` gets:

* level 3: compresses 40 million/s, decompresses 110 million/s, compression
  ratio 3.14.
* level 22: compresses 0.44 million/s, decompresses 170 million/s,
  compression ratio 3.51.

## Real World

Real world datasets are the best indicator of usefulness.
We have compared against 3 datasets, all of which are readily available and
accessible in download size:
* [Devin Smith's air quality data download](https://deephaven.io/wp-content/devinrsmith-air-quality.20220714.zstd.parquet) (15MB)
* [NYC taxi data (2023-04 high volume for hire)](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) (469MB)
* Reddit r/place 2022 data
  * [upstream Reddit post and original data](https://www.reddit.com/r/place/comments/txvk2d/rplace_datasets_april_fools_2022/)
  * [processed Parquet file download](https://pcodec-public.s3.amazonaws.com/reddit_2022_place_numerical.parquet) (1.3GB)

<div style="text-align:center">
  <img
    alt="bar charts showing better compression for pco than zstd.parquet"
    src="../images/real_world_compression_ratio.svg"
    width="600px"
  >
  <img
    alt="bar charts showing similar compression speed for pco and zstd.parquet"
    src="../images/real_world_compression_speed.svg"
    width="600px"
  >
  <img
    alt="bar charts showing faster decompression speed for pco than zstd.parquet"
    src="../images/real_world_decompression_speed.svg"
    width="600px"
  >
</div>

These were again done on a single core of an M3 performance core.
Only numerical columns (the physical dtypes INT32, INT64, FLOAT, and DOUBLE)
were used.
