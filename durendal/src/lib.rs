//! For crate-level documentation, see either
//! <https://crates.io/crates/q_compress> or
//! <https://github.com/mwlon/quantile-compression/tree/main/q_compress>.
#![allow(clippy::needless_range_loop)]
#![allow(clippy::manual_range_contains)]

#[doc = include_str!("../README.md")]
#[cfg(doctest)]
struct ReadmeDoctest;

pub use base_compressor::CompressorConfig;
pub use base_decompressor::DecompressorConfig;
pub use bin::Bin;
pub use chunk_metadata::{BinMetadata, ChunkMetadata};
pub use constants::DEFAULT_COMPRESSION_LEVEL;
pub use flags::Flags;

pub mod data_types;
pub mod errors;
pub mod standalone;
pub mod wrapped;

mod base_compressor;
mod base_decompressor;
mod bin;
mod bin_optimization;
mod bit_reader;
mod bit_words;
mod bit_writer;
mod bits;
mod body_decompressor;
mod chunk_metadata;
mod chunk_spec;
mod compression_table;
mod constants;
mod delta_encoding;
mod flags;
mod gcd_utils;
mod huffman_decoding;
mod huffman_encoding;
mod num_decompressor;
mod progress;
mod run_len_utils;

#[cfg(test)]
mod tests;
