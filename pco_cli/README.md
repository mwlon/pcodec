[![Crates.io][crates-badge]][crates-url]

[crates-badge]: https://img.shields.io/crates/v/pco_cli.svg

[crates-url]: https://crates.io/crates/pco_cli

# Setup

You can compress, decompress, and inspect standalone .pco files using the CLI.
Follow this setup:

1. Install Rust: https://www.rust-lang.org/tools/install
2. `cargo install pco_cli`

This provides you with the `pcodec` command.

# Command Info

You can always get help, e.g. `pcodec`, `pcodec compress --help`.

## Bench

This command runs benchmarks, taking in data you provide and printing out
compression time, decompression time, and compression ratio for whatever
codecs you request.

```shell
pcodec bench --parquet my_input_data.parquet
pcodec bench \
  --csv my_input_data.csv \
  --csv-has-header \
  --codecs pco:level=9,parquet:compression=zstd4 \
  --dtypes f32 \
  --datasets foo,bar \
  --iters 7 \
  --limit 999999 \
  --save-dir ./tmp
pcodec bench --binary-dir ./data
```

### Setting up synthetic data

One way to generate test data from a wide variety of processes and
distributions is from the `generate_randoms.py` script in the pcodec
repository.
To run it, set up a python3 environment with `numpy` installed.
In that environment, `cd`'d in to the root of the repo,
run `python pco_cli/generate_randoms.py`.
This will populate some human-readable data in `data/txt/` and
the exact same numerical data as bytes in `data/binary/`.

Unless other input is provided, `pcodec bench` will search the
`./data/binary/` path.

## Compress

This command compresses a single column of a .csv or .parquet file into a .pco
file.

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

## Decompress

This command prints numbers in a .pco file to stdout.

Examples:

```shell
pcodec decompress --limit 256 in.pco
```

## Inspect

This command prints out information about a .pco file.

Examples:

```shell
% pcodec inspect in.pco
```
