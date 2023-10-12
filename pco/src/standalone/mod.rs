pub use compressor::Compressor;
pub use decompressor::DecompressedItem;
pub use decompressor::Decompressor;
pub use simple::{auto_compress, auto_decompress, simple_compress, simple_decompress};

mod compressor;
mod constants;
mod decompressor;
mod simple;
