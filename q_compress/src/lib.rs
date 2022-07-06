//! For crate-level documentation, see either
//! <https://crates.io/crates/q_compress> or
//! <https://github.com/mwlon/quantile-compression/tree/main/q_compress>.
#![allow(clippy::needless_range_loop)]
#[doc = include_str!("../README.md")]

pub use auto::{auto_compress, auto_compressor_config, auto_decompress};
pub use chunk_metadata::{ChunkMetadata, PrefixMetadata};
pub use compressor::{Compressor, CompressorConfig};
pub use constants::DEFAULT_COMPRESSION_LEVEL;
pub use decompressor::{DecompressedItem, Decompressor, DecompressorConfig};
pub use flags::Flags;
pub use prefix::Prefix;

pub mod data_types;
pub mod errors;

mod auto;
mod bit_reader;
mod bit_words;
mod bit_writer;
mod bits;
mod chunk_body_decompressor;
mod chunk_metadata;
mod constants;
mod compression_table;
mod compressor;
mod decompressor;
mod delta_encoding;
mod flags;
mod gcd_utils;
mod huffman_decoding;
mod huffman_encoding;
mod num_decompressor;
mod prefix;
mod prefix_optimization;

#[cfg(test)]
mod tests;
