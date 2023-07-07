use anyhow::Result;
use std::io::Write;

use q_compress::data_types::NumberLike;
use q_compress::{Decompressor, Prefix, PrefixMetadata};

use crate::handlers::HandlerImpl;
use crate::opt::InspectOpt;
use crate::utils;

const INDENT: &str = "  ";

pub trait InspectHandler {
  fn header_byte(&self) -> u8; // only used for testing now
  fn inspect(&self, opt: &InspectOpt, bytes: &[u8]) -> Result<()>;
}

fn print_prefixes<T: NumberLike>(prefixes: &[Prefix<T>]) {
  println!("{}{} prefixes:", INDENT, prefixes.len());
  for p in prefixes {
    println!("{}{}{}", INDENT, INDENT, p);
  }
}

impl<T: NumberLike> InspectHandler for HandlerImpl<T> {
  fn header_byte(&self) -> u8 {
    T::HEADER_BYTE
  }

  fn inspect(&self, opt: &InspectOpt, bytes: &[u8]) -> Result<()> {
    println!("inspecting {:?}", opt.path);
    let mut decompressor = Decompressor::<T>::default();
    decompressor.write_all(bytes).unwrap();

    let flags = decompressor.header()?;
    println!("=================\n");
    println!("data type: {}", utils::dtype_name::<T>());
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
    let uncompressed_size = T::PHYSICAL_BITS / 8 * total_n;
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

    if total_n > 0 && flags.delta_encoding_order == 0 {
      let mut bounds = Vec::new();
      for meta in &metadatas {
        let prefs = match &meta.prefix_metadata {
          PrefixMetadata::Simple { prefixes } => prefixes,
          _ => panic!("expected simple metadata for delta encoding order 0"),
        };
        for pref in prefs {
          bounds.push(pref.lower);
          bounds.push(pref.upper);
        }
      }

      let min_num = bounds
        .iter()
        .min_by_key(|&&x| x.to_unsigned())
        .cloned()
        .unwrap();
      let max_num = bounds
        .iter()
        .max_by_key(|&&x| x.to_unsigned())
        .cloned()
        .unwrap();
      println!(
        "[min, max] numbers: [{}, {}]",
        min_num, max_num
      );
    }

    for (i, m) in metadatas.iter().enumerate() {
      println!("\nchunk {}", i);
      println!("{}n: {}", INDENT, m.n);
      match &m.prefix_metadata {
        PrefixMetadata::Simple { prefixes } => print_prefixes(prefixes),
        PrefixMetadata::Delta { prefixes, .. } => print_prefixes(prefixes),
      }
    }

    Ok(())
  }
}
