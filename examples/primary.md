# Quantile Compression Example

The `primary` example generates a wide variety of common distributions
with the `bool`, `f64`, `i64`, and `TimestampMicros` data types,
compresses them, decompresses them, and makes sure
all the data came back bitwise identical.
We also compare vs
gzip, Snappy, ZStandard, and their combinations with Parquet
on the binary data of these numbers.
On all parquet files, we use the max compression level available for the
codec (9 for gzip, 22 for zstd).

## Running

TL;DR:
* `python generate_randoms.py`
* `cargo run --release --example primary`

The script to generate the data uses python, so set up a python3
environment with `numpy` and `pyarrow` installed.
In that environment, `cd`'d into the `examples/` directory, run
`python generate_randoms.py`.
This will populate some human-readable data in `data/txt/` and
the exact same numerical data as bytes in `data/binary`.
For instance,
```
% head -5 data/txt/normal10.txt
-4
10
6
-11
4
```
shows integers sampled from a floored normal distribution with standard
deviation of 10.

Then to run quantile compression and decompression on each dataset, run
`cargo run --release --example primary`.
This will show the quantile parameters chosen for each dataset and how long
it took to compress and decompress.
You can see the compressed files in `data/q_compressed_$DEPTH`, where `DEPTH=6`
by default.

When generating randoms, some comparison file formats were already generated,
like `.gzip.parquet` in `data/gzip_parquet/`.

If you want to try out pure gzip on the same data,
make sure you have `gzip` and `xargs` installed,
then simply run `sh run_gzip.sh`.
This will use gzip to compress the binary version of the data at compression
levels 1 and 9.

To try pure Snappy,
you can install the `szip` and `xargs` commands and run `sh run_snappy.sh`.

## Comparing vs other algorithms

You can compare file sizes with `ls`:
```
% ls -lh data/q_compressed_6 | awk '{print $5 "\t" $9}'
122K	bool8_random.qco
122K	bool8_random_del=1.qco
4.2M	f64_edge_cases.qco
6.1M	f64_edge_cases_del=1.qco
6.6M	f64_normal_at_0.qco
5.4M	f64_normal_at_1000.qco
6.5M	f64_slow_cosine.qco
5.4M	f64_slow_cosine_del=1.qco
3.8M	f64_slow_cosine_del=2.qco
2.1M	f64_slow_cosine_del=7.qco
440K	i64_cents.qco
37B     i64_constant.qco
619K	i64_dollars.qco
122K	i64_extremes.qco
183K	i64_extremes_del=1.qco
2.6M	i64_geo1M.qco
248K	i64_geo2.qco
1.7M	i64_lomax05.qco
1.5M	i64_lomax15.qco
1.5M	i64_lomax25.qco
280K	i64_normal1.qco
666K	i64_normal10.qco
2.6M	i64_normal1M.qco
2.1M	i64_slow_cosine.qco
837K	i64_slow_cosine_del=1.qco
216K	i64_slow_cosine_del=2.qco
624K	i64_slow_cosine_del=7.qco
13K     i64_sparse.qco
1.3M	i64_total_cents.qco
7.6M	i64_uniform.qco
4.8M	micros_near_linear.qco
2.7M	micros_near_linear_del=1.qco

% ls -lh data/zstd_parquet | awk '{print $5 "\t" $9}' 
126K	bool8_random.zstd.parquet
5.1M	f64_edge_cases.zstd.parquet
7.6M	f64_normal_at_0.zstd.parquet
6.8M	f64_normal_at_1000.zstd.parquet
7.2M	f64_slow_cosine.zstd.parquet
578K	i64_cents.zstd.parquet
615B	i64_constant.zstd.parquet
834K	i64_dollars.zstd.parquet
126K	i64_extremes.zstd.parquet
3.5M	i64_geo1M.zstd.parquet
326K	i64_geo2.zstd.parquet
2.3M	i64_lomax05.zstd.parquet
1.8M	i64_lomax15.zstd.parquet
1.8M	i64_lomax25.zstd.parquet
264K	i64_normal1.zstd.parquet
796K	i64_normal10.zstd.parquet
3.6M	i64_normal1M.zstd.parquet
1.8M	i64_slow_cosine.zstd.parquet
16K     i64_sparse.zstd.parquet
1.3M	i64_total_cents.zstd.parquet
7.9M	i64_uniform.zstd.parquet
3.3M	micros_near_linear.zstd.parquet
```

Note that the uncompressed, binary file size for each of these datasets
is 7.6MB (1M numbers * 8 bytes / number).

For some distributions, we demonstrate different delta encoding orders
in the `.qco` files by indicating `del=x`.
The best delta encoding order can usually be known ahead of time, so use the
best compressed file for comparison purposes.

In the above `ls` commands,
you can see that `.qco` files are typically a good deal smaller
than their corresponding `.zstd.parquet` files,
even though we're comparing a fast `q_compress` compression level with the
very highest zstd compresison level.

Other than `.qco`, the best performing alternative was `.zstd.parquet`.
Some observations one can draw, comparing `.qco` to `.zstd.parquet`:
* In all cases `.qco` files are smaller.
  On average about 26% smaller.
* With uniformly random data, there's not really any information to compress,
  so both algorithms use close to the original file size of 7.6MB.
* Particularly interesting are the `cents`, `dollars`, and `total_cents`
  distributions, which are meant to model the distribution of prices
  at a retail store.
  The cents are commonly 99, 98, 0, etc.
  Quantile compression smooths over high-frequency information like this
  when just given total cents (100 + dollars + cents), and only compresses
  down to 1.3MB.
  But given the two columns separately, it compresses down to
  619K + 440K = 1.06MB.
* Floating point distributions can't be compressed as much as integers.
  That's because between any power of 2, 64 bit floats use 52 bits of
  information, which is already most of their 64 bits.
  In other words, even a fairly tight distribution of floats can have high
  entropy.
  Integer distributions have low entropy much more commonly.

