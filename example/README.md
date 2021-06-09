# Quantile Compression Example

This example generates a wide variety of common integer distributions
with the `i64` and `f64` data types,
compresses them, decompresses them, and makes sure
all the data came back bitwise identical.
We also compare vs `gzip` on the binary data of these integers.

## Running

TL;DR:
* `python generate_randoms.py`
* `cargo run --release`

The script to generate the data uses python, so set up a python3
environment with numpy installed.
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
`cargo run --release`.
This will show the quantile parameters chosen for each dataset and how long
it took to compress and decompress.
The timing benchmarks include time taken to write/read to disk.
You can see the compressed files in `data/q_compressed_$DEPTH`, where `DEPTH=6`
by default.

## Comparing vs gzip

To use gzip on the same data, make sure you have `gzip` and `xargs` installed,
then simply run `sh run_gzip.sh`.
This will use gzip to compress the binary version of the data at compression
levels 1 and 9.
To compare file sizes, you can just use `ls`.

```
% ls -lh data/q_compressed_6
... 439K ... f64_edge_cases.qco
... 681K ... f64_normal_at_0.qco
... 552K ... f64_normal_at_1000.qco
...  28B ... i64_constant.qco
...  12K ... i64_extremes.qco
... 262K ... i64_geo1M.qco
...  25K ... i64_geo2.qco
... 172K ... i64_lomax05.qco
... 156K ... i64_lomax15.qco
... 152K ... i64_lomax25.qco
...  28K ... i64_normal1.qco
...  67K ... i64_normal10.qco
... 270K ... i64_normal1M.qco
... 2.1K ... i64_sparse.qco
... 782K ... i64_uniform.qco
% ls -lh data/gzip_9         
... 505K ... f64_edge_cases.bin.gz
... 751K ... f64_normal_at_0.bin.gz
... 656K ... f64_normal_at_1000.bin.gz
... 1.2K ... i64_constant.bin.gz
...  22K ... i64_extremes.bin.gz
... 351K ... i64_geo1M.bin.gz
...  44K ... i64_geo2.bin.gz
... 245K ... i64_lomax05.bin.gz
... 230K ... i64_lomax15.bin.gz
... 226K ... i64_lomax25.bin.gz
...  47K ... i64_normal1.bin.gz
... 106K ... i64_normal10.bin.gz
... 361K ... i64_normal1M.bin.gz
... 2.4K ... i64_sparse.bin.gz
... 782K ... i64_uniform.bin.gz
```

Note that the uncompressed, binary file size for each of these datasets
is 781KB (100000 numbers * 8 bytes / number).

Here you can see that data is typically a good deal smaller
as `.qco` than `.gz`, even though we're comparing a fast
`.qco` compression level with the very highest
`.gz` compresison level.
Some observations:
* For most data, `.qco` files are only about 70% as big.
* For the degenerate case of constant data, 
they're less than 3% as big!
* With uniformly random data, there's not really any information to compress,
so both algorithms use nearly the exact binary data size of 781KB.
* Floating point distributions can't be compressed as much as integers.
That's because between any power of 2, 64 bit floats use 52 bits of
information, which is already most of their 64 bits.
In other words, even a fairly tight distribution of floats can have high
entropy.
Integer distributions have low entropy much more commonly.



Also, if you pay attention to the compression/decompression times, you
should find that quantile compression is several times faster than gzip.
