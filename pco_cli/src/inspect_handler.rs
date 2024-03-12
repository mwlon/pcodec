use anyhow::Result;

use pco::data_types::{Latent, NumberLike};
use pco::standalone::{FileDecompressor, MaybeChunkDecompressor};
use pco::{Bin, ChunkLatentVarMeta, ChunkMeta, Mode};

use crate::handlers::HandlerImpl;
use crate::inspect_handler::NumDisplay::{Delta, IntFloat, IntMult, IntMultAdj, Unsigned};
use crate::number_like_arrow::NumberLikeArrow;
use crate::opt::InspectOpt;
use crate::utils;

const INDENT: &str = "  ";

pub trait InspectHandler {
  fn inspect(&self, opt: &InspectOpt, bytes: &[u8]) -> Result<()>;
}

#[derive(Clone, Copy, Debug)]
enum NumDisplay<U: Latent> {
  Unsigned,
  Delta,
  IntFloat,
  IntMult(U),
  IntMultAdj(U),
}

fn print_bins<T: NumberLike>(bins: &[Bin<T::L>], display: NumDisplay<T::L>) {
  for bin in bins {
    let lower = bin.lower;
    let lower_str = match display {
      Unsigned => T::from_unsigned(lower).to_string(),
      Delta => {
        // deltas are "centered" around U::MID, so we want to uncenter and
        // display easily-readable signed numbers
        if lower < T::L::MID {
          format!("-{}", T::L::MID - lower)
        } else {
          (lower - T::L::MID).to_string()
        }
      }
      IntFloat => lower.to_int_float().to_string(),
      IntMult(base) => {
        let unsigned_0 = T::default().to_unsigned();
        let relative_to_0 = lower.wrapping_sub(unsigned_0 / base);
        T::from_unsigned(unsigned_0.wrapping_add(relative_to_0)).to_string()
      }
      IntMultAdj(base) => {
        let unsigned_0_rem = T::default().to_unsigned() % base;
        if lower < unsigned_0_rem {
          format!("-{}", unsigned_0_rem - lower)
        } else {
          (lower - unsigned_0_rem).to_string()
        }
      }
    };
    println!(
      "{}weight: {} lower: {} offset bits: {}",
      INDENT, bin.weight, lower_str, bin.offset_bits
    );
  }
}

fn measure_bytes_read(src: &[u8], prev_src_len: &mut usize) -> usize {
  let res = *prev_src_len - src.len();
  *prev_src_len = src.len();
  res
}

fn display_latent_var<T: NumberLike>(
  latent_idx: usize,
  meta: &ChunkMeta<T::L>,
  latent: &ChunkLatentVarMeta<T::L>,
) {
  let latent_name = match (meta.mode, latent_idx) {
    (Mode::Classic, 0) => "primary".to_string(),
    (Mode::FloatMult(config), 0) => format!("multiplier [x{}]", config.base),
    (Mode::FloatMult(_), 1) => "ULPs adjustment".to_string(),
    (Mode::IntMult(base), 0) => format!("multiplier [x{}]", base),
    (Mode::IntMult(_), 1) => "adjustment".to_string(),
    _ => panic!(
      "unknown latent: {:?}/{}",
      meta.mode, latent_idx
    ),
  };
  let display = match (
    meta.mode,
    latent_idx,
    meta.delta_encoding_order,
  ) {
    // so far delta order only applies to 0th latent var
    (_, 0, order) if order > 0 => Delta,
    (Mode::FloatMult(_), 0, _) => IntFloat,
    (Mode::FloatMult(_), 1, _) => Delta,
    (Mode::IntMult(base), 0, _) => IntMult(base),
    (Mode::IntMult(base), 1, _) => IntMultAdj(base),
    _ => Unsigned,
  };
  println!(
    "latent: {} n_bins: {} ANS size log: {}",
    latent_name,
    latent.bins.len(),
    latent.ans_size_log,
  );
  print_bins::<T>(&latent.bins, display);
}

impl<P: NumberLikeArrow> InspectHandler for HandlerImpl<P> {
  fn inspect(&self, opt: &InspectOpt, src: &[u8]) -> Result<()> {
    let mut prev_src_len_val = src.len();
    let prev_src_len = &mut prev_src_len_val;
    println!("inspecting {:?}", opt.path);
    let (fd, mut src) = FileDecompressor::new(src)?;
    let header_size = measure_bytes_read(src, prev_src_len);

    let version = fd.format_version();
    println!("=================\n");
    println!(
      "data type: {}",
      utils::dtype_name::<P::Num>()
    );
    println!("format version: {}", version,);

    let mut meta_size = 0;
    let mut page_size = 0;
    let mut footer_size = 0;
    let mut chunk_ns = Vec::new();
    let mut metas = Vec::new();
    let mut void = Vec::new();
    loop {
      // Rather hacky, but first just measure the metadata size,
      // then reread it to measure the page size
      match fd.chunk_decompressor::<P::Num, _>(src)? {
        MaybeChunkDecompressor::Some(cd) => {
          chunk_ns.push(cd.n());
          metas.push(cd.meta().clone());
          meta_size += measure_bytes_read(cd.into_src(), prev_src_len);
        }
        MaybeChunkDecompressor::EndOfData(rest) => {
          src = rest;
          footer_size += measure_bytes_read(src, prev_src_len);
          break;
        }
      }

      match fd.chunk_decompressor::<P::Num, _>(src)? {
        MaybeChunkDecompressor::Some(mut cd) => {
          void.resize(cd.n(), P::Num::default());
          let _ = cd.decompress(&mut void)?;
          src = cd.into_src();
          page_size += measure_bytes_read(src, prev_src_len);
        }
        _ => panic!("unreachable"),
      }
    }

    println!("number of chunks: {}", metas.len());
    let total_n: usize = chunk_ns.iter().sum();
    println!("total n: {}", total_n);
    let uncompressed_size = <P::Num as NumberLike>::L::BITS as usize / 8 * total_n;
    println!(
      "uncompressed byte size: {}",
      uncompressed_size
    );
    let compressed_size = header_size + meta_size + page_size + footer_size;
    println!(
      "compressed byte size: {} (ratio: {})",
      compressed_size,
      uncompressed_size as f64 / compressed_size as f64,
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
      src.len(),
    );

    for (i, meta) in metas.iter().enumerate() {
      println!(
        "\nchunk: {} n: {} delta order: {} mode: {:?}",
        i, chunk_ns[i], meta.delta_encoding_order, meta.mode,
      );
      for (latent_var_idx, latent_var_meta) in meta.per_latent_var.iter().enumerate() {
        display_latent_var::<P::Num>(latent_var_idx, meta, latent_var_meta);
      }
    }

    Ok(())
  }
}
