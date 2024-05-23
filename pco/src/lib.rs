//! For general information about pco (full name: Pcodec), including the
//! signifance of standalone vs. wrapped, see <https://github.com/mwlon/pcodec/>.
//!
//! # Quick Start
//!
//! ```rust
//! use pco::standalone::{simpler_compress, simple_decompress};
//! use pco::DEFAULT_COMPRESSION_LEVEL;
//! use pco::errors::PcoResult;
//!
//! fn main() -> PcoResult<()> {
//!   // your data
//!   let mut my_nums = Vec::new();
//!   for i in 0..100000 {
//!     my_nums.push(i as i64);
//!   }
//!
//!   // compress
//!   let compressed: Vec<u8> = simpler_compress(&my_nums, DEFAULT_COMPRESSION_LEVEL)?;
//!   println!("compressed down to {} bytes", compressed.len());
//!
//!   // decompress
//!   let recovered = simple_decompress::<i64>(&compressed)?;
//!   println!("got back {} ints from {} to {}", recovered.len(), recovered[0], recovered.last().unwrap());
//!   Ok(())
//! }
//! ```
//!
//! # API Notes
//!
//! * In some places, Pco methods accept a destination (either `W: Write` or `&mut [T: NumberLike]`).
//! If Pco returns an error, it is possible both the destination and the struct
//! have been modified.
//! * Pco will always try to process all numbers, and it will fail if insufficient bytes are
//! available. For instance, during decompression Pco will try to fill the entire `&mut [T]`
//! passed in, returning an insufficient data error if the `&[u8]` passed in is not long enough.

#![allow(clippy::uninit_vec)]

pub use auto::auto_delta_encoding_order;
pub use bin::Bin;
pub use chunk_config::{ChunkConfig, FloatMultSpec, FloatQuantSpec, IntMultSpec, PagingSpec};
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
mod float_quant_utils;
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
