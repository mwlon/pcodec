# Benchmarks

This generates a wide variety of common distributions
with the `bool`, `f64`, `i64`, and `TimestampMicros` data types,
compresses them, decompresses them, and makes sure
all the data came back bitwise identical.
We also compare vs
gzip, Snappy, ZStandard, and their combinations with Parquet
on the binary data of these numbers.
On all parquet files, we use the max compression level available for the
codec (9 for gzip, 22 for zstd).

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

Then to run quantile compression and decompression on each dataset, run
`cargo run --release --bin bench`.
This will show the compressed size and how long
it took to compress and decompress each dataset.
You can see the compressed files in
`bench/data/pco/`.

You can try different configurations as well as q_compress and
ZStandard on any subset of the
datasets by specifying arguments; e.g. the following runs 3 iterations of
* `q_compress` level 12 with delta encoding order 1 and GCD's off
* and `zstd` level 22

on any datasets whose names match "near_linear" or "slow_cosine":
```
cargo run --release --example primary -- \
  -c "qco:12:1:false,zstd:22" \
  -d "slow_cosine,near_linear", \
  -i 3
```

When generating randoms, some comparison file formats were already generated,
like `.zstd.parquet` in `bench/data/zstd_parquet/`.

To try pure gzip on the same data,
make sure you have `gzip` and `xargs` installed,
then simply run `sh bench/run_gzip.sh`.
This will use gzip to compress the binary version of the data at compression
levels 1 and 9.

To try pure Snappy,
you can install the `szip` and `xargs` commands and run
`sh bench/run_snappy.sh`.

## Comparing vs other algorithms

You can compare file sizes with `ls`:
```
% ls -lh bench/data/pco | awk '{print $5 "\t" $9}'
1.6M	f64_decimal_8:0:true.qco
2.0M	f64_diablo_long_8:0:true.qco
6.3K	f64_diablo_short_8:0:true.qco
3.6M	f64_integers_8:0:true.qco
6.6M	f64_normal_at_0_8:0:true.qco
4.2M	f64_normal_at_1M_8:0:true.qco
55B	f64_radians_8:1:true.qco
1.8M	f64_slow_cosine_8:5:true.qco
202K	i64_bad_huffman_8:0:true.qco
432K	i64_cents_8:0:true.qco
28B	i64_constant_8:0:true.qco
2.6M	i64_dist_shift_8:0:true.qco
605K	i64_dollars_8:0:true.qco
245K	i64_geo2_8:0:true.qco
1.2M	i64_interl0_8:0:true.qco
2.2M	i64_interl1_8:0:true.qco
2.2M	i64_interl_scrambl1_8:0:true.qco
1.6M	i64_lomax05_long_8:0:true.qco
5.3K	i64_lomax05_short_8:0:true.qco
1.5M	i64_lomax25_8:0:true.qco
185K	i64_slow_cosine_8:2:true.qco
9.9K	i64_sparse_8:0:true.qco
1.2M	i64_total_cents_8:0:true.qco
7.6M	i64_uniform_8:0:true.qco
3.6M	micros_millis_8:0:true.qco
2.7M	micros_near_linear_8:1:true.qco

% ls -lh bench/data/zstd_parquet | awk '{print $5 "\t" $9}'          
1.7M	f64_decimal.zstd.parquet
2.0M	f64_diablo_long.zstd.parquet
9.5K	f64_diablo_short.zstd.parquet
4.9M	f64_integers.zstd.parquet
7.6M	f64_normal_at_0.zstd.parquet
5.4M	f64_normal_at_1M.zstd.parquet
7.0M	f64_radians.zstd.parquet
7.5M	f64_slow_cosine.zstd.parquet
207K	i64_bad_huffman.zstd.parquet
606K	i64_cents.zstd.parquet
615B	i64_constant.zstd.parquet
3.2M	i64_dist_shift.zstd.parquet
888K	i64_dollars.zstd.parquet
345K	i64_geo2.zstd.parquet
1.3M	i64_interl0.zstd.parquet
1.9M	i64_interl1.zstd.parquet
2.3M	i64_interl_scrambl1.zstd.parquet
2.3M	i64_lomax05_long.zstd.parquet
11K	i64_lomax05_short.zstd.parquet
1.8M	i64_lomax25.zstd.parquet
1.8M	i64_slow_cosine.zstd.parquet
17K	i64_sparse.zstd.parquet
1.4M	i64_total_cents.zstd.parquet
7.9M	i64_uniform.zstd.parquet
5.9M	micros_millis.zstd.parquet
3.3M	micros_near_linear.zstd.parquet
```

In the above `ls` commands,
you can see that `.pco` files are typically a good deal smaller
than their corresponding `.zstd.parquet` files,
even though we're comparing a fast `q_compress` compression level with the
very highest zstd compresison level.

Other than `.pco` and `.qco`, the best performing alternative was `.zstd.parquet`.
Some observations one can draw, comparing `.pco` to `.zstd.parquet`:
* In all cases `.pco` files are smaller.
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

