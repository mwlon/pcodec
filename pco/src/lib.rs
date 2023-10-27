//! For crate-level documentation, see either
//! <https://crates.io/crates/pco> or
//! <https://github.com/mwlon/pcodec/tree/main/pco>.
#![allow(clippy::uninit_vec)]

pub use auto::auto_delta_encoding_order;
pub use bin::Bin;
pub use chunk_config::ChunkConfig;
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
mod bin;
mod bin_optimization;
mod bit_reader;
mod bit_writer;
mod bits;
mod chunk_metadata;
mod compression_table;
mod constants;
mod delta;
mod float_mult_utils;
mod format_version;
mod latent_batch_decompressor;
mod latent_batch_dissector;
mod modes;
mod progress;
mod unsigned_src_dst;

mod chunk_config;
mod io;
mod page_metadata;
mod read_write_uint;
#[cfg(test)]
mod tests;
