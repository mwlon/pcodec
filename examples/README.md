# Quantile Compression Example

This example generates a wide variety of common integer distributions
with the `i64` and `f64` data types,
compresses them, decompresses them, and makes sure
all the data came back bitwise identical.
We also compare vs
gzip, Snappy, and their combinations with Parquet
on the binary data of these numbers.

## Running

TL;DR:
* `python generate_randoms.py`
* `cargo run --release --example primary`

The script to generate the data uses python, so set up a python3
environment with `numpy` and `pyarrow` installed.
In that environment, `cd`'d into the `example/` directory, run
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
% ls -lh data/q_compressed_6 
... 4.3M ... f64_edge_cases.qco
... 6.6M ... f64_normal_at_0.qco
... 5.4M ... f64_normal_at_1000.qco
... 440K ... i64_cents.qco
...  28B ... i64_constant.qco
... 622K ... i64_dollars.qco
... 122K ... i64_extremes.qco
... 2.6M ... i64_geo1M.qco
... 248K ... i64_geo2.qco
... 1.7M ... i64_lomax05.qco
... 1.5M ... i64_lomax15.qco
... 1.5M ... i64_lomax25.qco
... 280K ... i64_normal1.qco
... 665K ... i64_normal10.qco
... 2.6M ... i64_normal1M.qco
...  13K ... i64_sparse.qco
... 1.3M ... i64_total_cents.qco
... 7.6M ... i64_uniform.qco

% ls -lh data/gzip_parquet  
... 5.2M ... f64_edge_cases.gzip.parquet
... 7.6M ... f64_normal_at_0.gzip.parquet
... 6.7M ... f64_normal_at_1000.gzip.parquet
... 603K ... i64_cents.gzip.parquet
... 632B ... i64_constant.gzip.parquet
... 895K ... i64_dollars.gzip.parquet
... 126K ... i64_extremes.gzip.parquet
... 3.8M ... i64_geo1M.gzip.parquet
... 348K ... i64_geo2.gzip.parquet
... 2.3M ... i64_lomax05.gzip.parquet
... 1.8M ... i64_lomax15.gzip.parquet
... 1.8M ... i64_lomax25.gzip.parquet
... 296K ... i64_normal1.gzip.parquet
... 796K ... i64_normal10.gzip.parquet
... 3.8M ... i64_normal1M.gzip.parquet
...  17K ... i64_sparse.gzip.parquet
... 1.4M ... i64_total_cents.gzip.parquet
... 7.9M ... i64_uniform.gzip.parquet
```

Note that the uncompressed, binary file size for each of these datasets
is 7.6MB (1M numbers * 8 bytes / number).

Here you can see that data is typically a good deal smaller
as `.qco` than `.gz`, even though we're comparing a fast
`.qco` compression level with the very highest
`.gz` compresison level.
Some observations:
* In all cases `.qco` files are smaller.
  On average about 25% smaller.
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
  622K + 440K = 1.06MB.
* Floating point distributions can't be compressed as much as integers.
  That's because between any power of 2, 64 bit floats use 52 bits of
  information, which is already most of their 64 bits.
  In other words, even a fairly tight distribution of floats can have high
  entropy.
  Integer distributions have low entropy much more commonly.

