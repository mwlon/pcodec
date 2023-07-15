use std::io::Write;

use anyhow::Result;

use pco::data_types::{NumberLike, UnsignedLike};
use pco::standalone::Decompressor;
use pco::{Bin, Mode};

use crate::handlers::HandlerImpl;
use crate::number_like_arrow::NumberLikeArrow;
use crate::opt::InspectOpt;
use crate::utils;

const INDENT: &str = "  ";

pub trait InspectHandler {
  fn inspect(&self, opt: &InspectOpt, bytes: &[u8]) -> Result<()>;
}

fn print_bins<T: NumberLike>(
  bins: &[Bin<T::Unsigned>],
  show_as_delta: bool,
  show_as_float_int: bool,
) {
  for bin in bins {
    let gcd_str = if bin.gcd == T::Unsigned::ONE {
      "".to_string()
    } else {
      format!(" [gcd: {}]", bin.gcd)
    };
    let lower_str = if show_as_delta {
      // hacky way to print the centered unsigned as a signed integer
      if bin.lower < T::Unsigned::MID {
        format!("-{}", T::Unsigned::MID - bin.lower)
      } else {
        (bin.lower - T::Unsigned::MID).to_string()
      }
    } else if show_as_float_int {
      bin.lower.to_int_float().to_string()
    } else {
      T::from_unsigned(bin.lower).to_string()
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
      println!(
        "\nchunk: {} n: {} mode: {:?}",
        i, m.n, m.mode
      );
      for (stream_idx, stream) in m.streams.iter().enumerate() {
        let stream_name = match (m.mode, stream_idx) {
          (Mode::Classic, 0) => "primary".to_string(),
          (Mode::Gcd, 0) => "primary".to_string(),
          (Mode::FloatMult(config), 0) => format!("multiplier [x{}]", config.base),
          (Mode::FloatMult(_), 1) => "ULPs adjustment".to_string(),
          _ => panic!("unknown stream: {:?}/{}", m.mode, stream_idx),
        };
        let show_as_float_int = matches!(m.mode, Mode::FloatMult { .. }) && stream_idx == 0;
        let show_as_delta = (matches!(m.mode, Mode::FloatMult { .. }) && stream_idx == 1)
          || m.delta_encoding_order > 0;
        println!(
          "stream: {} n_bins: {} ANS size log: {}",
          stream_name,
          stream.bins.len(),
          stream.ans_size_log,
        );
        print_bins::<P::Num>(
          &stream.bins,
          show_as_delta,
          show_as_float_int,
        );
      }
    }

    Ok(())
  }
}
