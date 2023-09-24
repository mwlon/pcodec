//! For crate-level documentation, see either
//! <https://crates.io/crates/pco> or
//! <https://github.com/mwlon/pcodec/tree/main/pco>.
#![allow(clippy::uninit_vec)]

pub use auto::auto_delta_encoding_order;
pub use base_compressor::CompressorConfig;
pub use base_decompressor::DecompressorConfig;
pub use bin::Bin;
pub use chunk_metadata::{ChunkLatentMetadata, ChunkMetadata};
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
mod chunk_metadata;
mod chunk_spec;
mod compression_table;
mod constants;
mod delta;
mod flags;
mod float_mult_utils;
mod lookback;
mod latent_batch_decompressor;
mod modes;
mod page_decompressor;
mod progress;
mod unsigned_src_dst;

#[cfg(test)]
mod tests;
