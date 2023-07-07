//! For crate-level documentation, see either
//! <https://crates.io/crates/pco> or
//! <https://github.com/mwlon/pcompress/tree/main/pco>.
#![allow(clippy::manual_range_contains)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::uninit_vec)]

pub use auto::auto_compressor_config;
pub use base_compressor::CompressorConfig;
pub use base_decompressor::DecompressorConfig;
pub use bin::Bin;
pub use chunk_metadata::ChunkMetadata;
pub use constants::DEFAULT_COMPRESSION_LEVEL;
pub use flags::Flags;
pub use modes::Mode;

#[doc = include_str!("../README.md")]
#[cfg(doctest)]
struct ReadmeDoctest;

pub mod data_types;
pub mod errors;
/// for compressing/decompressing .pco files
pub mod standalone;
/// for compressing/decompressing as part of an outer, wrapping format
pub mod wrapped;

mod ans;
mod auto;
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
mod float_mult_utils;
mod modes;
mod num_decompressor;
mod progress;
mod unsigned_src_dst;

#[cfg(test)]
mod tests;
