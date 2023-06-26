use anyhow::Result;
use std::io::Write;
use durendal::Bin;
use durendal::data_types::{NumberLike, UnsignedLike};
use durendal::standalone::Decompressor;

use crate::handlers::HandlerImpl;
use crate::number_like_arrow::NumberLikeArrow;
use crate::opt::InspectOpt;
use crate::utils;

const INDENT: &str = "  ";

pub trait InspectHandler {
  fn inspect(&self, opt: &InspectOpt, bytes: &[u8]) -> Result<()>;
}

fn print_bins<U: UnsignedLike>(bins: &[Bin<U>]) {
  println!("{}{} bins:", INDENT, bins.len());
  for bin in bins {
    println!("{}{}{}", INDENT, INDENT, bin);
  }
}

impl<P: NumberLikeArrow> InspectHandler for HandlerImpl<P> {
  fn inspect(&self, opt: &InspectOpt, bytes: &[u8]) -> Result<()> {
    println!("inspecting {:?}", opt.path);
    let mut decompressor = Decompressor::<P::Num>::default();
    decompressor.write_all(bytes).unwrap();

    let flags = decompressor.header()?;
    println!("=================\n");
    println!("data type: {}", utils::dtype_name::<P::Num>());
    println!("flags: {:?}", flags);
    let header_size = decompressor.bit_idx() / 8;
    let mut metadata_size = 0;

    let mut metadatas = Vec::new();
    let mut start_bit_idx = decompressor.bit_idx();
    while let Some(meta) = decompressor.chunk_metadata()? {
      let bit_idx = decompressor.bit_idx();
      metadata_size += (bit_idx - start_bit_idx) / 8;

      decompressor.skip_chunk_body()?;
      metadatas.push(meta);
      start_bit_idx = decompressor.bit_idx();
    }
    let compressed_size = decompressor.bit_idx() / 8;

    println!("number of chunks: {}", metadatas.len());
    let total_n: usize = metadatas.iter().map(|m| m.n).sum();
    println!("total n: {}", total_n);
    let uncompressed_size = P::Num::PHYSICAL_BITS / 8 * total_n;
    println!(
      "uncompressed byte size: {}",
      uncompressed_size
    );
    println!(
      "compressed byte size: {} (ratio: {})",
      compressed_size,
      uncompressed_size as f64 / compressed_size as f64,
    );
    println!("{}header size: {}", INDENT, header_size);
    println!(
      "{}chunk metadata size: {}",
      INDENT, metadata_size
    );
    println!(
      "{}chunk body size: {}",
      INDENT,
      metadatas
        .iter()
        .map(|m| m.compressed_body_size)
        .sum::<usize>()
    );
    println!("{}footer size: 1", INDENT);
    println!(
      "{}unknown trailing bytes: {}",
      INDENT,
      bytes.len() - compressed_size
    );

    for (i, m) in metadatas.iter().enumerate() {
      println!("\nchunk {}", i);
      println!("{}n: {}", INDENT, m.n);
      print_bins(&m.bins);
    }

    Ok(())
  }
}
