[![Crates.io][crates-badge]][crates-url]

[crates-badge]: https://img.shields.io/crates/v/q_compress_cli.svg
[crates-url]: https://crates.io/crates/q_compress_cli

# `q_compress` CLI

## Setup

You can compress, decompress, and inspect .qco files using our simple CLI.
Follow this setup:

1. Install Rust: https://www.rust-lang.org/tools/install
2. `cargo install q_compress_cli`

This provides you with the `qcompress` command.

## Command Info

You can always get help, e.g. `qcompress`, `qcompress compress --help`.

### Compress

This command compresses a single column of a .csv or .parquet file into a .qco
file.
If delta encoding order (`--delta-order`) is not specified, the default
behavior is to use the first numbers and make an educated guess for the best
delta encoding order.

Examples:

```shell
qcompress compress --csv my.csv --col-name my_column out.qco
qcompress compress --parquet my.snappy.parquet --col-name my_column out.qco

qcompress compress \
  --csv my.csv \
  --col-idx 0 \
  --csv-has-header \
  --dtype u32 \
  --level 7 \
  --overwrite \
  out.qco

qcompress compress \
  --csv time_series.csv \
  --csv-timestamp-format "%Y-%m-%d %H:%M:%S%.f%z" \
  --col-name time \
  --dtype TimestampMicros \
  --delta-order 1 \
  out.qco
qcompress compress \
  --csv time_series.csv \
  --col-name temperature \
  --dtype f32 \
  --delta-order 3 \
  out.qco
```

### Decompress

This command prints numbers in a .qco file to stdout.

Examples:

```shell
qcompress decompress --limit 10 in.qco
qcompress decompress --timestamp-format "%Y-%m-%d %H:%M:%S.%f" in.qco > out.txt
```

### Inspect

This command prints out information about a .qco file.

Examples:

```shell
% qcompress inspect in.qco
...
inspecting "in.qco"
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
...
```

## Versioning

The major and minor semver versions of this crate are meant to match that of
`q_compress`.
However, the patch version is not related to `q_compress`'s patch version.
