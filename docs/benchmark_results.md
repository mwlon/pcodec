# Results

## Real World

Real world datasets are the best indicator of usefulness.
We have compared against 3 datasets, all of which are readily available and
accessible in download size:

* [Devin Smith's air quality data download](https://deephaven.io/wp-content/devinrsmith-air-quality.20220714.zstd.parquet) (
  15MB)
* [NYC taxi data (2023-04 high volume for hire)](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) (469MB)
* Reddit r/place 2022 data
  * [upstream Reddit post and original data](https://www.reddit.com/r/place/comments/txvk2d/rplace_datasets_april_fools_2022/)
  * [processed Parquet file download](https://pcodec-public.s3.amazonaws.com/reddit_2022_place_numerical.parquet) (
    1.3GB)

| dataset     | uncompressed size | numeric data types |
|-------------|-------------------|--------------------|
| air quality | 59.7MB            | i32, i64           |
| taxi        | 2.14GB            | f64, i32, i64      |
| r/place     | 4.19GB            | i32, i64           |

<div style="text-align:center">
  <img
    alt="bar charts showing better compression for Pco than zstd.parquet"
    src="../images/real_world_compression_ratio.svg"
    width="700px"
  >
  <img
    alt="bar charts showing similar compression speed for Pco and zstd.parquet"
    src="../images/real_world_compression_speed.svg"
    width="700px"
  >
  <img
    alt="bar charts showing faster decompression speed for Pco than zstd.parquet"
    src="../images/real_world_decompression_speed.svg"
    width="700px"
  >
</div>

These were again done on a single core of an M3 performance core.
Only numerical columns (the physical dtypes INT32, INT64, FLOAT, and DOUBLE)
were used.
For Blosc, the SHUFFLE filter and the Zstd default of Zstd level 3 was used.
For Parquet, the Parquet default of Zstd level 1 was used.

## Synthetic

Speeds are reported in count of numbers compressed or decompressed
per second with 2 significant figures.
Compression ratio is reported with 3 significant figures.

| dataset           | compression speed / (million/s) | decompression speed / (million/s) | compression ratio |
|-------------------|---------------------------------|-----------------------------------|-------------------|
| `f64_decimal`     | 47                              | 320                               | 4.67              |
| `f64_slow_cosine` | 48                              | 300                               | 4.51              |
| `i64_lomax05`     | 68                              | 600                               | 4.63              |
| `i64_sparse`      | 180                             | 770                               | 780               |
| `micros_millis`   | 70                              | 940                               | 2.13              |

`i64` and `f64` are each 8 bytes, so compression is around 300-500MB/s,
and decompression is around 2-5GB/s.

All figures reported here are calculated using a single thread on an Apple
M3 performance core, operating on in-memory data, using Rust 1.73.
Benchmarks were done by taking the median of 100 runs on a dataset of 1M
numbers with `compression_level` 8.
