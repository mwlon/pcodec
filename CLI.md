# q_compress CLI

## Setup

You can compress and inspect .qco files using our simple CLI.
Until we publish to package managers, follow this setup:

1. Install Rust: https://www.rust-lang.org/tools/install
2. `git clone https://github.com/mwlon/quantile-compression.git`
3. `cd quantile-compression`

## Example Commands

You can always get help, e.g. `cargo run --release`, `cargo run compress --help`.

### Compress

This command compresses a single column of a .csv or .parquet file into a .qco
file.
If delta encoding order (`--delta-order`) is not specified, the default
behavior is to use the first numbers and make an educated guess for the best
delta encoding order.

```shell
cargo run --release compress --csv my.csv --col-name my_column out.qco
cargo run --release compress --parquet my.snappy.parquet --col-name my_column out.qco

cargo run --release compress \
  --csv my.csv \
  --col-idx 0 \
  --csv-has-header \
  --dtype u32 \
  --level 7 \
  --overwrite \
  out.qco

cargo run --release compress \
  --csv time_series.csv \
  --csv-timestamp-format "%Y-%m-%d %H:%M:%s%.f%z" \
  --col-name time \
  --dtype TimestampMicros \
  --delta-order 1 \
  out.qco
cargo run --release compress \
  --csv time_series.csv \
  --col-name temperature \
  --dtype f32 \
  --delta-order 3 \
  out.qco
```

### Inspect

This command prints out information about a .qco file.

```shell
% cargo run --release inspect out.qco
...
inspecting "out.qco"
=================

data type: f64
flags: Flags { use_5_bit_prefix_len: true, delta_encoding_order: 0 }
number of chunks: 1
total n: 1000000
uncompressed byte size: 8000000
compressed byte size: 6967210 (ratio: 1.1482358074465964)
	header size: 6
	chunk metadata size: 602
	chunk body size: 6966601
	footer size: 1
	unknown trailing bytes: 0
[min, max] numbers: [-4.628380674508539, 4.919770799153994]
```