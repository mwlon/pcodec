use std::collections::BTreeMap;

use anyhow::Result;
use serde::Serialize;
use tabled::settings::object::Columns;
use tabled::settings::{Alignment, Modify, Style};
use tabled::{Table, Tabled};

use pco::data_types::{Latent, NumberLike};
use pco::metadata::ChunkMeta;
use pco::standalone::{FileDecompressor, MaybeChunkDecompressor};

use crate::core_handlers::CoreHandlerImpl;
use crate::dtypes::PcoNumberLike;
use crate::inspect::InspectOpt;
use crate::utils;

pub trait InspectHandler {
  fn inspect(&self, opt: &InspectOpt, bytes: &[u8]) -> Result<()>;
}

#[derive(Serialize)]
pub struct CompressionSummary {
  pub ratio: f64,
  pub total_size: usize,
  pub header_size: usize,
  pub meta_size: usize,
  pub page_size: usize,
  pub footer_size: usize,
  pub unknown_trailing_bytes: usize,
}

#[derive(Tabled)]
pub struct BinSummary {
  weight: u32,
  lower: String,
  offset_bits: u32,
}

#[derive(Serialize)]
pub struct LatentVarSummary {
  n_bins: usize,
  ans_size_log: u32,
  bins: String,
}

#[derive(Serialize)]
pub struct ChunkSummary {
  idx: usize,
  n: usize,
  mode: String,
  delta_order: usize,
  // using BTreeMaps to preserve ordering
  latent_vars: BTreeMap<String, LatentVarSummary>,
}

#[derive(Serialize)]
pub struct Output {
  pub filename: String,
  pub data_type: String,
  pub format_version: u8,
  pub n: usize,
  pub n_chunks: usize,
  pub uncompressed_size: usize,
  pub compressed: CompressionSummary,
  pub chunks: Vec<ChunkSummary>,
}

fn measure_bytes_read(src: &[u8], prev_src_len: &mut usize) -> usize {
  let res = *prev_src_len - src.len();
  *prev_src_len = src.len();
  res
}

fn build_latent_var_summaries<T: NumberLike>(
  meta: &ChunkMeta<T::L>,
) -> BTreeMap<String, LatentVarSummary> {
  let describers = T::get_latent_describers(meta);
  let mut summaries = BTreeMap::new();
  for (latent_var_idx, latent_var_meta) in meta.per_latent_var.iter().enumerate() {
    let describer = &describers[latent_var_idx];
    let unit = describer.latent_units();

    let mut bins = Vec::new();
    for bin in &latent_var_meta.bins {
      bins.push(BinSummary {
        weight: bin.weight,
        lower: format!("{}{}", describer.latent(bin.lower), unit),
        offset_bits: bin.offset_bits,
      });
    }
    let bins_table = Table::new(bins)
      .with(Style::rounded())
      .with(Modify::new(Columns::new(0..3)).with(Alignment::right()))
      .to_string();

    let summary = LatentVarSummary {
      n_bins: latent_var_meta.bins.len(),
      ans_size_log: latent_var_meta.ans_size_log,
      bins: bins_table.to_string(),
    };

    summaries.insert(describer.latent_var(), summary);
  }

  summaries
}

impl<T: PcoNumberLike> InspectHandler for CoreHandlerImpl<T> {
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
      match fd.chunk_decompressor::<T, _>(src)? {
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

      match fd.chunk_decompressor::<T, _>(src)? {
        MaybeChunkDecompressor::Some(mut cd) => {
          void.resize(cd.n(), T::default());
          let _ = cd.decompress(&mut void)?;
          src = cd.into_src();
          page_size += measure_bytes_read(src, prev_src_len);
        }
        _ => panic!("unreachable"),
      }
    }

    let n: usize = chunk_ns.iter().sum();
    let uncompressed_size = <T as NumberLike>::L::BITS as usize / 8 * n;
    let compressed_size = header_size + meta_size + page_size + footer_size;
    let unknown_trailing_bytes = src.len();

    let mut chunks = Vec::new();
    for (idx, meta) in metas.iter().enumerate() {
      let latent_vars = build_latent_var_summaries::<T>(meta);
      chunks.push(ChunkSummary {
        idx,
        n: chunk_ns[idx],
        mode: format!("{:?}", meta.mode),
        delta_order: meta.delta_encoding_order,
        latent_vars,
      });
    }

    let output = Output {
      filename: opt.path.to_str().unwrap().to_string(),
      data_type: utils::dtype_name::<T>(),
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
      chunks,
    };

    println!("{}", toml::to_string_pretty(&output)?);

    Ok(())
  }
}
