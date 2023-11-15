pub use compressor::{ChunkCompressor, FileCompressor};
pub use decompressor::{ChunkDecompressor, FileDecompressor, MaybeChunkDecompressor};
pub use simple::{auto_compress, auto_decompress, simple_compress};

mod compressor;
mod constants;
mod decompressor;
mod simple;
