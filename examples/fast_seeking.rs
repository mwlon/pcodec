use q_compress::{BitWriter, F64Compressor, F64Decompressor, BitReader};
use rand::Rng;
use std::time::Instant;

fn main() {
  let mut writer = BitWriter::default();

  let compressor = F64Compressor::default();
  compressor.header(&mut writer).expect("header");
  let n_chunks = 10;
  let mut rng = rand::thread_rng();

  for _ in 0..n_chunks {
    let mut nums = Vec::new();
    for _ in 0..100000 {
      nums.push(rng.gen::<f64>());
    }
    compressor.compress_chunk(&nums, &mut writer).expect("write chunk");
  }
  compressor.footer(&mut writer).expect("footer");

  // now read back only the metadata
  let start_t = Instant::now();
  let mut reader = BitReader::from(writer.pop());
  let decompressor = F64Decompressor::default();
  let flags = decompressor.header(&mut reader).expect("flags");
  let mut metadatas = Vec::new();
  while let Some(meta) = decompressor.chunk_metadata(&mut reader, &flags).expect("read chunk") {
    reader.seek_bytes(meta.compressed_body_size);
    metadatas.push(meta);
  }

  let n: usize = metadatas.iter()
    .map(|meta| meta.n)
    .sum();
  println!("seeked through {} entries in {:?}", n, Instant::now() - start_t);
}