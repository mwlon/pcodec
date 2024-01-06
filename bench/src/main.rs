#![allow(clippy::uninit_vec)]

use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::ops::AddAssign;
use std::path::Path;
use std::time::Duration;

use clap::Parser;
use parquet::basic::Type;
use parquet::column::reader::get_typed_column_reader;
use parquet::file::reader::{FileReader, SerializedFileReader};
use tabled::settings::object::Columns;
use tabled::settings::{Alignment, Modify, Style};
use tabled::{Table, Tabled};

use opt::Opt;

use crate::codecs::CodecConfig;
use crate::dtypes::Dtype;
use crate::num_vec::NumVec;

mod codecs;
mod dtypes;
pub mod num_vec;
mod opt;

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

#[derive(Clone, Default, Tabled)]
struct PrintStat {
  pub dataset: String,
  pub codec: String,
  #[tabled(display_with = "display_duration")]
  pub compress_dt: Duration,
  #[tabled(display_with = "display_duration")]
  pub decompress_dt: Duration,
  pub compressed_size: usize,
}

impl AddAssign for PrintStat {
  fn add_assign(&mut self, rhs: Self) {
    self.compressed_size += rhs.compressed_size;
    self.compress_dt += rhs.compress_dt;
    self.decompress_dt += rhs.decompress_dt;
  }
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

fn handle(num_vec: &NumVec, dataset: String, config: &CodecConfig, opt: &Opt) -> PrintStat {
  println!("\n{} x {}", dataset, config);
  let save_fname = format!(
    "{}{}.{}",
    &dataset,
    config.details(),
    config.inner.name(),
  );
  let precomputed = config
    .inner
    .warmup_iter(num_vec, &save_fname, &opt.handler_opt);
  let mut benches = Vec::with_capacity(opt.iters);
  for _ in 0..opt.iters {
    benches.push(
      config
        .inner
        .stats_iter(num_vec, &precomputed, &opt.handler_opt),
    );
  }
  PrintStat::compute(dataset, config.to_string(), &benches)
}

fn get_dataset_and_dtype(synthetic_path: &Path) -> (String, String) {
  let dataset = basename_no_ext(synthetic_path);
  let dtype = dataset.split('_').next().unwrap().to_string();
  (dataset, dtype)
}

fn handle_synthetic(path: &Path, config: &CodecConfig, opt: &Opt) -> PrintStat {
  let (dataset, dtype) = get_dataset_and_dtype(path);

  let raw_bytes = fs::read(path).expect("could not read");
  let num_vec = NumVec::new(&dtype, raw_bytes);
  handle(&num_vec, dataset, config, opt)
}

fn collect_parquet_num_vec<T: Dtype>(
  pq_reader: &SerializedFileReader<File>,
  col_idx: usize,
  n: usize,
) -> NumVec {
  let mut res = Vec::with_capacity(n);
  let mut def_levels = Vec::with_capacity(n);
  let mut rep_levels = Vec::with_capacity(n);
  unsafe {
    res.set_len(n);
    def_levels.set_len(n);
    rep_levels.set_len(n);
  }

  let mut start = 0;
  for i in 0..pq_reader.metadata().num_row_groups() {
    let row_group_reader = pq_reader.get_row_group(i).unwrap();
    let mut col_reader =
      get_typed_column_reader::<T::Parquet>(row_group_reader.get_column_reader(col_idx).unwrap());

    let (n_records_read, _, _) = col_reader
      .read_records(
        usize::MAX,
        Some(&mut def_levels),
        Some(&mut rep_levels),
        &mut res[start..],
      )
      .unwrap();
    start += n_records_read
  }

  T::num_vec(T::vec_from_parquet(res))
}

fn handle_parquet_column(
  pq_reader: &SerializedFileReader<File>,
  col_idx: usize,
  n: usize,
  opt: &Opt,
) -> Vec<PrintStat> {
  let pq_meta = pq_reader.metadata();
  let pq_col = pq_meta.file_metadata().schema_descr().column(col_idx);
  let num_vec = match pq_col.physical_type() {
    Type::INT32 => collect_parquet_num_vec::<i32>(pq_reader, col_idx, n),
    Type::INT64 => collect_parquet_num_vec::<i64>(pq_reader, col_idx, n),
    Type::FLOAT => collect_parquet_num_vec::<f32>(pq_reader, col_idx, n),
    Type::DOUBLE => collect_parquet_num_vec::<f64>(pq_reader, col_idx, n),
    _ => return vec![],
  };

  let dtype = num_vec.dtype_str();
  if !opt.includes_dtype(dtype) {
    return vec![];
  }

  let mut stats = Vec::new();
  let dataset = format!("{}_{}", dtype, pq_col.name());
  for codec in &opt.codecs {
    stats.push(handle(
      &num_vec,
      dataset.to_string(),
      codec,
      opt,
    ));
  }

  stats
}

fn handle_parquet_dataset(path: &Path, opt: &Opt) -> Vec<PrintStat> {
  let file = File::open(path).unwrap();
  let pq_reader = SerializedFileReader::new(file).unwrap();
  let pq_meta = pq_reader.metadata();
  let pq_schema = pq_meta.file_metadata().schema_descr();

  let n_cols = pq_schema.num_columns();

  let mut n = 0;
  for row_group_meta in pq_meta.row_groups() {
    n += row_group_meta.num_rows() as usize;
  }

  let mut stats = Vec::new();

  for col_idx in 0..n_cols {
    stats.extend(handle_parquet_column(
      &pq_reader, col_idx, n, opt,
    ));
  }
  stats
}

fn print_stats(mut stats: Vec<PrintStat>, opt: &Opt) {
  if stats.is_empty() {
    println!("No datasets found that match filters!");
    return;
  }

  let mut aggregate = PrintStat::default();
  let mut aggregate_by_codec: HashMap<String, PrintStat> = HashMap::new();
  for stat in &stats {
    aggregate += stat.clone();
    aggregate_by_codec
      .entry(stat.codec.clone())
      .or_default()
      .add_assign(stat.clone());
  }
  stats.extend(opt.codecs.iter().map(|codec| {
    let codec = codec.to_string();
    let mut stat = aggregate_by_codec.get(&codec).cloned().unwrap();
    stat.codec = codec;
    stat
  }));
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
  let synthetic_paths = if opt.parquet_dataset.is_some() {
    vec![]
  } else {
    let mut synthetic_paths = files
      .into_iter()
      .map(|f| f.unwrap().path())
      .filter(|path| {
        let (dataset, dtype) = get_dataset_and_dtype(path);
        opt.includes_dtype(&dtype) && opt.includes_dataset(&dataset)
      })
      .collect::<Vec<_>>();
    synthetic_paths.sort();
    synthetic_paths
  };

  let mut stats = Vec::new();
  for path in synthetic_paths {
    for config in &opt.codecs {
      stats.push(handle_synthetic(&path, config, &opt));
    }
  }

  if let Some(parquet_dataset) = opt.parquet_dataset.as_ref() {
    stats.extend(handle_parquet_dataset(parquet_dataset, &opt));
  }

  print_stats(stats, &opt);
}
