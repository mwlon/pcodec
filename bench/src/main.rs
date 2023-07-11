#![allow(clippy::useless_transmute)]
#![allow(clippy::uninit_vec)]

mod codecs;
mod dtypes;
pub mod num_vec;
mod opt;

use std::fs;

use std::path::Path;
use std::time::Duration;

use clap::Parser;
use tabled::settings::object::Columns;
use tabled::settings::{Alignment, Modify, Style};
use tabled::{Table, Tabled};

use crate::codecs::CodecConfig;
use crate::num_vec::NumVec;
use opt::Opt;

const BASE_DIR: &str = "bench/data";
// if this delta order is specified, use a dataset-specific order

#[derive(Clone, Default)]
pub struct BenchStat {
  pub compress_dt: Duration,
  pub decompress_dt: Duration,
  pub compressed_size: usize,
}

fn median_duration(mut durations: Vec<Duration>) -> Duration {
  durations.sort_unstable();
  let lo = durations[(durations.len() - 1) / 2];
  let hi = durations[durations.len() / 2];
  (lo + hi) / 2
}

fn display_duration(duration: &Duration) -> String {
  format!("{:?}", duration)
}

#[derive(Tabled, Default)]
struct PrintStat {
  pub dataset: String,
  pub codec: String,
  #[tabled(display_with = "display_duration")]
  pub compress_dt: Duration,
  #[tabled(display_with = "display_duration")]
  pub decompress_dt: Duration,
  pub compressed_size: usize,
}

impl PrintStat {
  fn compute(dataset: String, codec: String, benches: &[BenchStat]) -> Self {
    let compressed_size = benches[0].compressed_size;
    let compress_dts = benches
      .iter()
      .map(|bench| bench.compress_dt)
      .collect::<Vec<_>>();
    let decompress_dts = benches
      .iter()
      .map(|bench| bench.decompress_dt)
      .collect::<Vec<_>>();

    PrintStat {
      dataset,
      codec,
      compressed_size,
      compress_dt: median_duration(compress_dts),
      decompress_dt: median_duration(decompress_dts),
    }
  }
}

fn basename_no_ext(path: &Path) -> String {
  let basename = path
    .file_name()
    .expect("weird path")
    .to_str()
    .expect("not unicode");
  match basename.find('.') {
    Some(i) => basename[..i].to_string(),
    _ => basename.to_string(),
  }
}

pub struct Precomputed {
  compressed: Vec<u8>,
  dtype: String,
}

fn handle(path: &Path, config: &CodecConfig, opt: &Opt) -> PrintStat {
  let dataset = basename_no_ext(path);
  let dtype = dtypes::dtype_str(&dataset);

  let fname = format!(
    "{}{}.{}",
    &dataset,
    config.details(),
    config.inner.name(),
  );
  let raw_bytes = fs::read(path).expect("could not read");
  let num_vec = NumVec::new(dtype, raw_bytes);
  let precomputed = config
    .inner
    .warmup_iter(&num_vec, &dataset, &fname, &opt.handler_opt);
  let mut benches = Vec::with_capacity(opt.iters);
  for _ in 0..opt.iters {
    benches.push(
      config
        .inner
        .stats_iter(&num_vec, &precomputed, &opt.handler_opt),
    );
  }
  PrintStat::compute(dataset, config.to_string(), &benches)
}

fn print_stats(mut stats: Vec<PrintStat>) {
  let mut aggregate = PrintStat::default();
  for stat in &stats {
    aggregate.compressed_size += stat.compressed_size;
    aggregate.compress_dt += stat.compress_dt;
    aggregate.decompress_dt += stat.decompress_dt;
  }
  stats.push(aggregate);
  let table = Table::new(stats)
    .with(Style::rounded())
    .with(Modify::new(Columns::new(2..)).with(Alignment::right()))
    .to_string();
  println!("{}", table);
}

fn main() {
  let opt: Opt = Opt::parse();

  let files = fs::read_dir(format!("{}/binary", BASE_DIR)).expect("couldn't read");
  let mut paths = files
    .into_iter()
    .map(|f| f.unwrap().path())
    .collect::<Vec<_>>();
  paths.sort();

  let mut stats = Vec::new();
  for path in paths {
    let path_str = path.to_str().unwrap();
    let keep = opt.datasets.is_empty()
      || opt
        .datasets
        .iter()
        .any(|dataset| path_str.contains(dataset));
    if !keep {
      continue;
    }

    for config in &opt.codecs {
      stats.push(handle(&path, config, &opt));
    }
  }

  print_stats(stats);
}
