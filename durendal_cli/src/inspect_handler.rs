use anyhow::Result;
use durendal::data_types::{NumberLike, UnsignedLike};
use durendal::standalone::Decompressor;
use durendal::{Bin, DynMode};
use std::io::Write;

use crate::handlers::HandlerImpl;
use crate::number_like_arrow::NumberLikeArrow;
use crate::opt::InspectOpt;
use crate::utils;

const INDENT: &str = "  ";

pub trait InspectHandler {
  fn inspect(&self, opt: &InspectOpt, bytes: &[u8]) -> Result<()>;
}

fn print_bins<T: NumberLike>(bins: &[Bin<T::Unsigned>], delta_encoded: bool, use_float_mult: bool) {
  for bin in bins {
    let gcd_str = if bin.gcd == T::Unsigned::ONE {
      "".to_string()
    } else {
      format!(" [gcd: {}]", bin.gcd)
    };
    let lower_str = if delta_encoded {
      // hacky way to print the centered unsigned as a signed integer
      if bin.lower < T::Unsigned::MID {
        format!("-{}", T::Unsigned::MID - bin.lower)
      } else {
        (bin.lower - T::Unsigned::MID).to_string()
      }
    } else {
      if use_float_mult {
        bin.lower.to_int_float().to_string()
      } else {
        T::from_unsigned(bin.lower).to_string()
      }
    };
    println!(
      "{}weight: {} lower: {} offset bits: {}{}",
      INDENT, bin.weight, lower_str, bin.offset_bits, gcd_str
    );
  }
}

impl<P: NumberLikeArrow> InspectHandler for HandlerImpl<P> {
  fn inspect(&self, opt: &InspectOpt, bytes: &[u8]) -> Result<()> {
    println!("inspecting {:?}", opt.path);
    let mut decompressor = Decompressor::<P::Num>::default();
    decompressor.write_all(bytes).unwrap();

    let flags = decompressor.header()?;
    println!("=================\n");
    println!(
      "data type: {}",
      utils::dtype_name::<P::Num>()
    );
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
      let (use_float_mult, float_mult_str) = match m.dyn_mode {
        DynMode::FloatMult { base, .. } => (true, format!(" [float mult: {}]", base)),
        _ => (false, "".to_string()),
      };
      println!(
        "\nchunk: {} n: {} n_bins: {} ANS size log: {}{}",
        i,
        m.n,
        m.bins.len(),
        m.ans_size_log,
        float_mult_str,
      );
      print_bins::<P::Num>(&m.bins, flags.delta_encoding_order > 0, use_float_mult);
    }

    Ok(())
  }
}
