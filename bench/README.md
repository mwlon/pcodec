# Benchmarks

This generates a wide variety of common distributions,
compresses them, decompresses them, and makes sure
all the data came back bitwise identical.
It supports
* multiple codecs (pco, q_compress, zstd)
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

Some simple results are in [benchmarks.md](./benchmarks.md).
