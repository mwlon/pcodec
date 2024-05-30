[![crates.io][crates-badge]][crates-url]
[![pypi.org][pypi-badge]][pypi-url]

[crates-badge]: https://img.shields.io/crates/v/pco.svg

[crates-url]: https://crates.io/crates/pco

[pypi-badge]: https://img.shields.io/pypi/v/pcodec.svg

[pypi-url]: https://pypi.org/project/pcodec/

# Pcodec

<div style="text-align:center">
  <img
    alt="bar charts showing better compression for Pco than zstd parquet or blosc"
    src="images/real_world_compression_ratio.svg"
    width="700px"
  >
</div>

Pcodec (or Pco, pronounced "pico") losslessly compresses and decompresses
numerical sequences with
[high compression ratio and fast speed](docs/benchmark_results.md).

**Use cases include:**

* columnar data
* long-term time series data
* serving numerical data to web clients
* low-bandwidth communication

**Data types:**
`u16`, `u32`, `u64`, `i16`, `i32`, `i64`, `f16`, `f32`, `f64`

## Get Started

[Use the CLI](./pco_cli/README.md) (also supports benchmarking)

[Use the Rust API](./pco/README.md)

[Use the Python API](./pco_python/README.md)

## How is Pco so much better than alternatives?

Pco is designed specifically for numerical data, whereas alternatives rely on
general-purpose (LZ) compressors that were designed for string or binary data.
Pco uses a holistic, 3-step approach:

* **modes**.
  Pco identifies an approximate structure of the numbers called a
  mode and then applies it to all the numbers.
  As an example, if all numbers are approximately multiples of 777, int mult mode
  decomposes each number `x` into latent variables `l_0` and
  `l_1` such that `x = 777 * l_0 + l_1`.
  Most natural data uses classic mode, which simply matches `x = l_0`.
* **delta enoding**.
  Pco identifies whether certain latent variables would be better compressed as
  consecutive deltas (or deltas of deltas, or so forth).
  If so, it takes consecutive differences.
* **binning**.
  This is the heart and most novel part of Pco.
  Pco represents each (delta-encoded) latent variable as an approximate,
  entropy-coded bin paired an exact offset into that bin.
  This nears the Shannon entropy of any smooth distribution very efficiently.

These 3 steps cohesively capture most entropy of numerical data without waste.

In contrast, LZ compressors are only effective for patterns like repeating
exact sequences of numbers.
Such patterns constitute just a small fraction of most numerical data's
entropy.

## Two ways to use it: wrapped or standalone

Pco is designed to be easily wrapped into another format.
It provides a powerful wrapped API with the building blocks to interleave it
with the wrapping format.
This is useful if the wrapping format needs to support things like nullability,
multiple columns, random access or seeking.

The standalone format is a minimal implementation of a wrapped format.
It supports batched decompression only with no other niceties.
It is mainly recommended for quick proofs of concept and benchmarking.

### Granularity

Pco has a hierarchy of multiple batches per page; multiple pages per chunk; and
multiple chunks per file.

|       | unit of ___                     | size for good compression |
|-------|---------------------------------|---------------------------|
| chunk | compression                     | \>10k numbers             |
| page  | interleaving w/ wrapping format | \>1k numbers              |
| batch | decompression                   | 256 numbers (fixed)       |

## Extra

### Docs

[benchmarks: see the results](docs/benchmark_results.md)

[format specification](./docs/format.md)

[terminology](./docs/terminology.md)

[Quantile Compression: Pcodec's predecessor](./quantile-compression/README.md)

[contributing guide](./docs/CONTRIBUTING.md)

### Community

[join the Discord](https://discord.gg/f6eRXgMP8w)

