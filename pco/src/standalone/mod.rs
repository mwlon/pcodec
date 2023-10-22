pub use compressor::{FileCompressor, ChunkCompressor};
pub use decompressor::{FileDecompressor, ChunkDecompressor};
pub use simple::{auto_compress, auto_decompress, simple_compress};

mod compressor;
mod constants;
mod decompressor;
mod simple;
