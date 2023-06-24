pub use auto::{auto_compress, auto_compressor_config, auto_decompress};
pub use compressor::Compressor;
pub use decompressor::DecompressedItem;
pub use decompressor::Decompressor;
pub use simple::{simple_compress, simple_decompress};

mod auto;
mod compressor;
mod decompressor;
mod simple;

