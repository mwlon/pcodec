use anyhow::Result;

use pco::data_types::{NumberLike, UnsignedLike};
use pco::standalone::FileDecompressor;
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
    let (fd, mut consumed) = FileDecompressor::new(bytes)?;

    let version = fd.format_version();
    println!("=================\n");
    println!(
      "data type: {}",
      utils::dtype_name::<P::Num>()
    );
    println!("format version: {}", version,);
    let header_size = consumed;

    let mut meta_size = 0;
    let mut page_size = 0;
    let mut footer_size = 0;
    let mut chunk_ns = Vec::new();
    let mut metas = Vec::new();
    let mut void = Vec::new();
    loop {
      let (maybe_cd, additional) = fd.chunk_decompressor::<P::Num>(&bytes[consumed..])?;
      consumed += additional;
      if let Some(mut cd) = maybe_cd {
        meta_size += additional;
        chunk_ns.push(cd.n());
        metas.push(cd.meta().clone());

        void.resize(cd.n(), P::Num::default());
        let (_, additional) = cd.decompress(&bytes[consumed..], &mut void)?;
        consumed += additional;
        page_size += additional;
      } else {
        footer_size += additional;
        break;
      }
    }

    println!("number of chunks: {}", metas.len());
    let total_n: usize = chunk_ns.iter().sum();
    println!("total n: {}", total_n);
    let uncompressed_size = P::Num::PHYSICAL_BITS / 8 * total_n;
    println!(
      "uncompressed byte size: {}",
      uncompressed_size
    );
    println!(
      "compressed byte size: {} (ratio: {})",
      consumed,
      uncompressed_size as f64 / consumed as f64,
    );
    println!("{}header size: {}", INDENT, header_size);
    println!(
      "{}chunk metadata size: {}",
      INDENT, meta_size
    );
    println!("{}page size: {}", INDENT, page_size,);
    println!("{}footer size: {}", INDENT, footer_size);
    println!(
      "{}unknown trailing bytes: {}",
      INDENT,
      bytes.len() - consumed
    );

    for (i, meta) in metas.iter().enumerate() {
      println!(
        "\nchunk: {} n: {} delta order: {} mode: {:?}",
        i, chunk_ns[i], meta.delta_encoding_order, meta.mode,
      );
      for (latent_idx, latent) in meta.latents.iter().enumerate() {
        let latent_name = match (meta.mode, latent_idx) {
          (Mode::Classic, 0) => "primary".to_string(),
          (Mode::Gcd, 0) => "primary".to_string(),
          (Mode::FloatMult(config), 0) => format!("multiplier [x{}]", config.base),
          (Mode::FloatMult(_), 1) => "ULPs adjustment".to_string(),
          _ => panic!("unknown latent: {:?}/{}", meta.mode, latent_idx),
        };
        let show_as_float_int = matches!(meta.mode, Mode::FloatMult { .. }) && latent_idx == 0;
        let show_as_delta = (matches!(meta.mode, Mode::FloatMult { .. }) && latent_idx == 1)
          || meta.delta_encoding_order > 0;
        println!(
          "latent: {} n_bins: {} ANS size log: {}",
          latent_name,
          latent.bins.len(),
          latent.ans_size_log,
        );
        print_bins::<P::Num>(
          &latent.bins,
          show_as_delta,
          show_as_float_int,
        );
      }
    }

    Ok(())
  }
}
