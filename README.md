[Click here for Quantile Compression](./quantile-compression/README.md).

# Pcodec

<div style="text-align:center">
  <img
    alt="bar charts showing better compression for pco than zstd.parquet"
    src="images/real_world_compression_ratio.svg"
    width="600px"
  >
</div>

Pcodec (or pco, pronounced "pico") losslessly compresses and decompresses
numerical sequences with
[high compression ratio and fast speed](./bench/README.md).

**Use cases include:**
* columnar data
* long-term time series data
* serving numerical data to web clients
* low-bandwidth communication

**Data types:**
`u32`, `u64`, `i32`, `i64`, `f32`, `f64`

It is also possible to implement your own data type via `NumberLike` and (if
necessary) `UnsignedLike` and `FloatLike`.
For timestamps or smaller integers, it is probably best to simply cast to one
of the natively supported data types.

## Get Started

[Use the CLI](./pco_cli/README.md)

[Use the Rust API](./pco/README.md)

## Performance and Compression Ratio

See [the benchmarks](./bench/README.md) to run the benchmark suite
or see its results.

## File Format

<img alt="pco wrapped format diagram" title="pco wrapped format" src="./images/wrapped_format.svg" />

The core idea of pco is to represent numbers as approximate, entropy-coded bins
paired with exact offsets into those bins.
Depending on the mode, there may be up to 2 streams of these bin-offset
pairings.

Pco is mainly meant to be wrapped into another format for production use cases.
It has a hierarchy of multiple batches per page; multiple pages per chunk; and
multiple chunks per file.

|       | unit of ___                     | size for good compression |
|-------|---------------------------------|---------------------------|
| chunk | compression                     | \>20k numbers             |
| page  | interleaving w/ wrapping format | \>1k numbers              |
| batch | decompression                   | 256 numbers (fixed)       |

The standalone format is a minimal implementation of a wrapped format.
It supports batched decompression only; no nullability, multiple
columns, random access, seeking, or other niceties.
It is mainly useful for quick proofs of concept and benchmarking.

<img alt="pco compression and decompression steps" title="compression and decompression steps" src="./images/processing.svg" />

## Contributing

[see CONTRIBUTING.md](./docs/CONTRIBUTING.md)

## Extra

[join the Discord](https://discord.gg/f6eRXgMP8w)
[terminology](./docs/terminology.md)
