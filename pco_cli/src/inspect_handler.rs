use anyhow::Result;

use pco::data_types::{Latent, NumberLike};
use pco::standalone::{FileDecompressor, MaybeChunkDecompressor};
use pco::{ChunkLatentVarMeta, ChunkMeta, Mode};

use crate::handlers::HandlerImpl;
use crate::number_like_arrow::NumberLikeArrow;
use crate::opt::InspectOpt;
use crate::utils;

const INDENT: &str = "  ";

pub trait InspectHandler {
  fn inspect(&self, opt: &InspectOpt, bytes: &[u8]) -> Result<()>;
}

fn measure_bytes_read(src: &[u8], prev_src_len: &mut usize) -> usize {
  let res = *prev_src_len - src.len();
  *prev_src_len = src.len();
  res
}

fn display_latent_var<T: NumberLike>(
  latent_var_idx: usize,
  meta: &ChunkMeta<T::L>,
  latent: &ChunkLatentVarMeta<T::L>,
) {
  let latent_var_name = match (meta.mode, latent_var_idx) {
    (Mode::Classic, 0) => "primary".to_string(),
    (Mode::FloatMult(base_latent), 0) => format!(
      "multiplier [x{}]",
      T::latent_to_string(base_latent, Mode::Classic, 0, 0)
    ),
    (Mode::FloatMult(_), 1) => "ULPs adjustment".to_string(),
    (Mode::IntMult(base), 0) => format!("multiplier [x{}]", base),
    (Mode::IntMult(_), 1) => "adjustment".to_string(),
    _ => panic!(
      "unknown latent: {:?}/{}",
      meta.mode, latent_var_idx
    ),
  };
  println!(
    "latent var: {} n_bins: {} ANS size log: {}",
    latent_var_name,
    latent.bins.len(),
    latent.ans_size_log,
  );

  for bin in &latent.bins {
    let lower_str = T::latent_to_string(
      bin.lower,
      meta.mode,
      latent_var_idx,
      meta.delta_encoding_order,
    );
    println!(
      "{}weight: {} lower: {} offset bits: {}",
      INDENT, bin.weight, lower_str, bin.offset_bits
    );
  }
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
