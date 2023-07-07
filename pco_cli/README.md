# `pcodec` CLI 

## Setup

You can compress, decompress, and inspect .pco files using our simple CLI.
Follow this setup:

1. Install Rust: https://www.rust-lang.org/tools/install
2. `cargo install pco_cli`

This provides you with the `pcodec` command.

## Command Info

You can always get help, e.g. `pcodec`, `pcodec compress --help`.

### Compress

This command compresses a single column of a .csv or .parquet file into a .pco
file.
If delta encoding order (`--delta-order`) is not specified, the default
behavior is to use the first numbers and make an educated guess for the best
delta encoding order.

Examples:

```shell
pcodec compress --csv my.csv --col-name my_column out.pco
pcodec compress --parquet my.snappy.parquet --col-name my_column out.pco

pcodec compress \
  --csv my.csv \
  --col-idx 0 \
  --csv-has-header \
  --dtype u32 \
  --level 7 \
  --overwrite \
  out.pco

pcodec compress \
  --csv time_series.csv \
  --col-name temperature \
  --dtype f32 \
  --delta-order 3 \
  out.pco
```

### Decompress

This command prints numbers in a .pco file to stdout.

Examples:

```shell
pcodec decompress --limit 10 in.pco
```

### Inspect

This command prints out information about a .pco file.

Examples:

```shell
% pcodec inspect in.pco
```

## Versioning

The major and minor semver versions of this crate are meant to match that of
`pco`.
However, the patch version is not related to `pco`'s patch version.
