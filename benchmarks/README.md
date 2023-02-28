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

TL;DR (`cd`'d into `quantile-compression/`):
* `python q_compress/examples/generate_randoms.py`
* `cargo run --release --example primary`

The script to generate the data uses python, so set up a python3
environment with `numpy` and `pyarrow` installed.
In that environment, run
`python q_compress/examples/generate_randoms.py`.
This will populate some human-readable data in `q_compress/examples/data/txt/` and
the exact same numerical data as bytes in `q_compress/examples/data/binary/`.
For instance,
```
% head -5 q_compress/examples/data/txt/f64_normal_at_0.txt
1.4159442981360018
-0.7575599825222276
0.11351170269868066
0.6510141768675483
-0.4418627064838288
```
shows floats sampled from a standard normal distribution.

Then to run quantile compression and decompression on each dataset, run
`cargo run --release --example primary`.
This will show the compressed size and how long
it took to compress and decompress each dataset.
You can see the compressed files in
`q_compress/examples/data/qco/`.

You can try different configurations as well as ZStandard on any subset of the
datasets by specifying arguments; e.g. the following runs 3 iterations of
* `q_compress` level 12 with delta encoding order 1 and GCD's off
* and `zstd` level 22

on any datasets whose names match "near_linear" or "slow_cosine":
```
cargo run --release --example primary -- \
  -c "qco:12:1:off,zstd:22" \
  -d "slow_cosine,near_linear", \
  -i 3
```

When generating randoms, some comparison file formats were already generated,
like `.zstd.parquet` in `q_compress/examples/data/zstd_parquet/`.

To try pure gzip on the same data,
make sure you have `gzip` and `xargs` installed,
then simply run `sh q_compress/examples/run_gzip.sh`.
This will use gzip to compress the binary version of the data at compression
levels 1 and 9.

To try pure Snappy,
you can install the `szip` and `xargs` commands and run
`sh q_compress/examples/run_snappy.sh`.

## Comparing vs other algorithms

You can compare file sizes with `ls`:
```
% ls -lh q_compress/examples/data/qco | awk '{print $5 "\t" $9}'
122K    bool_random_6:0:true.qco
4.2M    f64_edge_cases_6:0:true.qco
3.6M    f64_integers_6:0:true.qco
6.6M    f64_normal_at_0_6:0:true.qco
5.4M    f64_normal_at_1000_6:0:true.qco
2.1M    f64_slow_cosine_6:7:true.qco
441K    i64_cents_6:0:true.qco
37B     i64_constant_6:0:true.qco
620K    i64_dollars_6:0:true.qco
122K    i64_extremes_6:0:true.qco
2.6M    i64_geo1M_6:0:true.qco
248K    i64_geo2_6:0:true.qco
1.7M    i64_lomax05_6:0:true.qco
1.5M    i64_lomax15_6:0:true.qco
1.5M    i64_lomax25_6:0:true.qco
666K    i64_normal10_6:0:true.qco
2.6M    i64_normal1M_6:0:true.qco
280K    i64_normal1_6:0:true.qco
216K    i64_slow_cosine_6:2:true.qco
13K     i64_sparse_6:0:true.qco
1.3M    i64_total_cents_6:0:true.qco
7.6M    i64_uniform_6:0:true.qco
3.6M    micros_millis_6:0:true.qco
2.7M    micros_near_linear_6:1:true.qco

% ls -lh q_compress/examples/data/zstd_parquet | awk '{print $5 "\t" $9}'          
126K    bool_random.zstd.parquet
5.1M    f64_edge_cases.zstd.parquet
4.6M    f64_integers.zstd.parquet
7.6M    f64_normal_at_0.zstd.parquet
6.8M    f64_normal_at_1000.zstd.parquet
7.2M    f64_slow_cosine.zstd.parquet
571K    i64_cents.zstd.parquet
615B    i64_constant.zstd.parquet
832K    i64_dollars.zstd.parquet
126K    i64_extremes.zstd.parquet
3.6M    i64_geo1M.zstd.parquet
325K    i64_geo2.zstd.parquet
2.3M    i64_lomax05.zstd.parquet
1.8M    i64_lomax15.zstd.parquet
1.8M    i64_lomax25.zstd.parquet
264K    i64_normal1.zstd.parquet
797K    i64_normal10.zstd.parquet
3.5M    i64_normal1M.zstd.parquet
1.8M    i64_slow_cosine.zstd.parquet
16K     i64_sparse.zstd.parquet
1.3M    i64_total_cents.zstd.parquet
7.9M    i64_uniform.zstd.parquet
5.3M    micros_millis.zstd.parquet
3.3M    micros_near_linear.zstd.parquet
```

In the above `ls` commands,
you can see that `.qco` files are typically a good deal smaller
than their corresponding `.zstd.parquet` files,
even though we're comparing a fast `q_compress` compression level with the
very highest zstd compresison level.

Other than `.qco`, the best performing alternative was `.zstd.parquet`.
Some observations one can draw, comparing `.qco` to `.zstd.parquet`:
* In all cases `.qco` files are smaller.
  On average about 27% smaller.
* With uniformly random data, there's not really any information to compress,
  so both algorithms use close to the original file size of 7.6MB.
* Particularly interesting are the `cents`, `dollars`, and `total_cents`
  distributions, which are meant to model the distribution of prices
  at a retail store.
  The cents are commonly 99, 98, 0, etc.
  Quantile compression smooths over high-frequency information like this
  when just given total cents (100 * dollars + cents), and only compresses
  down to 1.29MB.
  But given the two columns separately, it compresses down to
  620K + 441K = 1.04MB.
* However, if you run at the max `q_compress` level of 12
  (`cargo run --release --example primary 12`),
  total cents drops to about 1.04MB, whereas dollars and cents separately
  stay at 1.02MB.
  So some suboptimal choices of data model can be compensated for via
  increased compression level.
* Some float distributions can't be compressed much.
  That's because between any power of 2, 64 bit floats use 52 bits of
  information, which is already most of their 64 bits.
  In other words, even a fairly tight distribution of floats can have high
  entropy.
  Integer distributions have low entropy much more commonly.

