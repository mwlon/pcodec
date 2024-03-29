use anyhow::Result;

use pco::data_types::{Latent, NumberLike};
use pco::standalone::{FileDecompressor, MaybeChunkDecompressor};
use pco::{ChunkLatentVarMeta, ChunkMeta, Mode};
use serde::Serialize;
use tabled::settings::object::Columns;
use tabled::settings::{Alignment, Modify, Style};
use tabled::{Table, Tabled};

use crate::handlers::HandlerImpl;
use crate::number_like_arrow::NumberLikeArrow;
use crate::opt::InspectOpt;
use crate::utils;

pub trait InspectHandler {
  fn inspect(&self, opt: &InspectOpt, bytes: &[u8]) -> Result<()>;
}

#[derive(Serialize)]
struct CompressionSummary {
  ratio: f64,
  total_size: usize,
  header_size: usize,
  meta_size: usize,
  page_size: usize,
  footer_size: usize,
  unknown_trailing_bytes: usize,
}

#[derive(Tabled)]
struct BinSummary {
  weight: u32,
  lower: String,
  offset_bits: u32,
}

#[derive(Serialize)]
struct LatentVarSummary {
  name: String,
  n_bins: usize,
  ans_size_log: u32,
  bins: String,
}

#[derive(Serialize)]
struct ChunkSummary {
  idx: usize,
  n: usize,
  mode: String,
  delta_order: usize,
  latent_var: Vec<LatentVarSummary>,
}

#[derive(Serialize)]
struct Output {
  filename: String,
  data_type: String,
  format_version: u8,
  n: usize,
  n_chunks: usize,
  uncompressed_size: usize,
  compressed: CompressionSummary,
  chunk: Vec<ChunkSummary>,
}

fn measure_bytes_read(src: &[u8], prev_src_len: &mut usize) -> usize {
  let res = *prev_src_len - src.len();
  *prev_src_len = src.len();
  res
}

fn build_latent_var_summary<T: NumberLike>(
  latent_var_idx: usize,
  meta: &ChunkMeta<T::L>,
  latent_var: &ChunkLatentVarMeta<T::L>,
) -> LatentVarSummary {
  let name = match (meta.mode, latent_var_idx) {
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

  let mut bins = Vec::new();
  for bin in &latent_var.bins {
    bins.push(BinSummary {
      weight: bin.weight,
      lower: T::latent_to_string(
        bin.lower,
        meta.mode,
        latent_var_idx,
        meta.delta_encoding_order,
      ),
      offset_bits: bin.offset_bits,
    });
  }

  let bins_table = Table::new(bins)
    .with(Style::rounded())
    .with(Modify::new(Columns::new(0..3)).with(Alignment::right()))
    .to_string();

  LatentVarSummary {
    name,
    n_bins: latent_var.bins.len(),
    ans_size_log: latent_var.ans_size_log,
    bins: bins_table.to_string(),
  }
}

impl<P: NumberLikeArrow> InspectHandler for HandlerImpl<P> {
  fn inspect(&self, opt: &InspectOpt, src: &[u8]) -> Result<()> {
    let mut prev_src_len_val = src.len();
    let prev_src_len = &mut prev_src_len_val;
    let (fd, mut src) = FileDecompressor::new(src)?;
    let header_size = measure_bytes_read(src, prev_src_len);

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

    let n: usize = chunk_ns.iter().sum();
    let uncompressed_size = <P::Num as NumberLike>::L::BITS as usize / 8 * n;
    let compressed_size = header_size + meta_size + page_size + footer_size;
    let unknown_trailing_bytes = src.len();

    let mut chunk = Vec::new();
    for (i, meta) in metas.iter().enumerate() {
      let mut latent_var = Vec::new();
      for (latent_var_idx, latent_var_meta) in meta.per_latent_var.iter().enumerate() {
        latent_var.push(build_latent_var_summary::<P::Num>(
          latent_var_idx,
          meta,
          latent_var_meta,
        ));
      }
      chunk.push(ChunkSummary {
        idx: i,
        n: chunk_ns[i],
        mode: format!("{:?}", meta.mode),
        delta_order: meta.delta_encoding_order,
        latent_var,
      })
    }

    let output = Output {
      filename: opt.path.to_str().unwrap().to_string(),
      data_type: utils::dtype_name::<P::Num>(),
      format_version: fd.format_version(),
      n,
      n_chunks: metas.len(),
      uncompressed_size,
      compressed: CompressionSummary {
        ratio: uncompressed_size as f64 / compressed_size as f64,
        total_size: compressed_size,
        header_size,
        meta_size,
        page_size,
        footer_size,
        unknown_trailing_bytes,
      },
      chunk,
    };

    println!("{}", toml::to_string_pretty(&output)?);

    Ok(())
  }
}
