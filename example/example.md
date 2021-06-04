# Quantile Compression Example

This example generates a wide variety of common integer distributions
with the `i64` data type,
compresses them, and decompresses them.
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
You can see the compressed files in `data/q_compressed_$DEPTH`, where `DEPTH=5`
by default.

## Comparing vs gzip

To use gzip on the same data, make sure you have `gzip` and `xargs` installed,
then simply run `sh run_gzip.sh`.
This will use gzip to compress the binary version of the data at compression
levels 1 and 9.
To compare file sizes, you can just use `ls`.

```
% ls -lh data/q_compressed_5 
...  23B ... constant.qco
...  12K ... extremes.qco
... 263K ... geo1M.qco
...  25K ... geo2.qco
... 175K ... lomax05.qco
... 157K ... lomax15.qco
... 153K ... lomax25.qco
...  28K ... normal1.qco
...  67K ... normal10.qco
... 271K ... normal1M.qco
... 781K ... uniform.qco

% ls -lh data/gzip_9        
... 1.2K ... constant.bin.gz
...  22K ... extremes.bin.gz
... 352K ... geo1M.bin.gz
...  44K ... geo2.bin.gz
... 246K ... lomax05.bin.gz
... 230K ... lomax15.bin.gz
... 226K ... lomax25.bin.gz
...  47K ... normal1.bin.gz
... 106K ... normal10.bin.gz
... 361K ... normal1M.bin.gz
... 782K ... uniform.bin.gz
```

Here you can see that data is typically a good deal smaller
as `.qco` than `.gz`.
For most data, `.qco` files are only about 70% as big,
and for the degenerate case of constant data, 
they're only 2% as big!
With uniformly random data, there's not really an information to compress,
so both algorithms use nearly the exact binary data size of 781KB.

Also, if you pay attention to the compression/decompression times, you
should find that quantile compression is several times faster than gzip.
