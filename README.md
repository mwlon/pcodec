[Click here for Quantile Compression](./quantile-compression/README.md).

# Pcodec

Pcodec (or pco, pronounced "pico") losslessly compresses and decompresses
numerical sequences
with high compression ratio and moderately fast speed.

**Use cases:**
* columnar data
* long-term time series data
* low-bandwidth communication

**Features:**
* wrapped format for interleaving within another format
* lossless; preserves ordering and exact bit representation
* nth-order delta encoding
* compresses faster or slower depending on compression level from 0 to 12
* fully streaming decompression

**Data types:**
`u32`, `u64`, `i32`, `i64`, `f32`, `f64`

It is also possible to implement your own data type via `NumberLike` and (if
necessary) `UnsignedLike` and `FloatLike`.
For smaller integers or timestamps, it is best to simply case to one of the
natively supported data types.

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

Pco is mainly meant to be wrapped into another format for production use cases,
using data pages as the unit of interleaving.
The standalone format supports only streaming decompression and seeking, but
not nullability, multiple columns, random access, or other niceties.

<img alt="pco compression and decompression steps" title="compression and decompression steps" src="./images/processing.svg" />

## Etymology

The names pcodec and pco were chosen for these reasons:
* "Pico" suggests that it makes very small things.
* Pco is reminiscent of qco, its preceding format.
* Pco is reminiscent of PancakeDB (Pancake COmpressed). Though PancakeDB is now
  history, it had a good name.
* Pcodec is short, provides some semantic meaning, and should be easy to
  search for.

The names are used for these purposes:
* pco => the library and data format
* pco_cli => the binary crate name
* pcodec => the binary CLI and the repo

## Extra

[join the Discord](https://discord.gg/f6eRXgMP8w)
