//! For crate-level documentation, see either
//! <https://crates.io/crates/pco> or
//! <https://github.com/mwlon/pcodec/tree/main/pco>.
#![allow(clippy::uninit_vec)]

pub use auto::auto_delta_encoding_order;
pub use compressor_config::CompressorConfig;
pub use bin::Bin;
pub use chunk_metadata::{ChunkLatentMetadata, ChunkMetadata};
pub use constants::{DEFAULT_COMPRESSION_LEVEL, FULL_BATCH_SIZE};
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
mod bin;
mod bin_optimization;
mod bit_reader;
mod bit_writer;
mod bits;
mod chunk_metadata;
mod chunk_spec;
mod compression_table;
mod constants;
mod delta;
mod format_version;
mod float_mult_utils;
mod latent_batch_decompressor;
mod latent_batch_dissector;
mod modes;
mod progress;
mod unsigned_src_dst;

#[cfg(test)]
mod tests;
mod page_metadata;
mod compressor_config;
mod read_write_uint;
