pub use compressor::{ChunkCompressor, FileCompressor};
pub use decompressor::{ChunkDecompressor, FileDecompressor, MaybeChunkDecompressor};
pub use dtype_or_termination::DataTypeOrTermination;
pub use simple::{simple_compress, simple_decompress, simple_decompress_into, simpler_compress};

mod compressor;
mod constants;
mod decompressor;
mod dtype_or_termination;
pub mod guarantee;
mod simple;
