use q_compress::{Compressor, Decompressor};
use rand::Rng;
use std::io::Write;
use std::time::Instant;

fn main() {
  let mut compressor = Compressor::<f64>::default();
  compressor.header().expect("header");
  let n_chunks = 10;
  let mut rng = rand::thread_rng();

  for _ in 0..n_chunks {
    let mut nums = Vec::new();
    for _ in 0..100000 {
      nums.push(rng.gen::<f64>());
    }
    compressor.chunk(&nums).expect("write chunk");
  }
  compressor.footer().expect("footer");

  // now read back only the metadata
  let bytes = compressor.drain_bytes();
  let mut decompressor = Decompressor::<f64>::default();
  decompressor.write_all(&bytes).unwrap();
  let start_t = Instant::now();
  decompressor.header().expect("flags");
  let mut metadatas = Vec::new();
  while let Some(meta) = decompressor.chunk_metadata().expect("read chunk") {
    metadatas.push(meta);
    decompressor.skip_chunk_body().expect("skipping");
  }

  let n: usize = metadatas.iter().map(|meta| meta.n).sum();
  println!(
    "seeked through {} entries in {:?}",
    n,
    Instant::now() - start_t
  );
}
