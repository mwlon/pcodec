//! For crate-level documentation, see
//! <https://github.com/mwlon/pcodec/tree/main/pco>.
#![allow(clippy::uninit_vec)]

pub use auto::auto_delta_encoding_order;
pub use bin::Bin;
pub use chunk_config::{ChunkConfig, FloatMultSpec, IntMultSpec, PagingSpec};
pub use chunk_meta::{ChunkLatentVarMeta, ChunkMeta};
pub use constants::{DEFAULT_COMPRESSION_LEVEL, DEFAULT_MAX_PAGE_N, FULL_BATCH_N};
pub use mode::Mode;
pub use progress::Progress;

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
mod chunk_config;
mod chunk_meta;
mod compression_intermediates;
mod compression_table;
mod constants;
mod delta;
mod float_mult_utils;
mod format_version;
mod histograms;
mod int_mult_utils;
mod latent_batch_decompressor;
mod latent_batch_dissector;
mod mode;
mod page_meta;
mod progress;
mod read_write_uint;
mod sampling;
mod sort_utils;

#[cfg(test)]
mod tests;
