//! For crate-level documentation, see either
//! <https://crates.io/crates/q_compress> or
//! <https://github.com/mwlon/quantile-compression>.

pub use bit_reader::BitReader;
pub use bit_writer::BitWriter;
pub use chunk_metadata::{ChunkMetadata, DecompressedChunk, PrefixMetadata};
pub use compressor::{Compressor, CompressorConfig};
pub use decompressor::{Decompressor, DecompressorConfig};
pub use flags::Flags;
pub use prefix::Prefix;

pub mod data_types;
pub mod errors;

mod bit_reader;
mod bit_writer;
mod bits;
mod chunk_metadata;
mod constants;
mod compression_table;
mod compressor;
mod decompressor;
mod delta_encoding;
mod flags;
mod huffman_decoding;
mod huffman_encoding;
mod prefix;
mod prefix_optimization;

#[cfg(test)]
mod tests;
