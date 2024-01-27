pub use compressor::{ChunkCompressor, FileCompressor};
pub use decompressor::{ChunkDecompressor, FileDecompressor, MaybeChunkDecompressor};
pub use dtype_or_termination::DataTypeOrTermination;
pub use simple::{auto_compress, auto_decompress, simple_compress, simple_decompress_into};

mod compressor;
mod constants;
mod decompressor;
mod dtype_or_termination;
mod simple;
