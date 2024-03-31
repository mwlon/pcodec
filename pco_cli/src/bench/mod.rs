#![allow(clippy::uninit_vec)]

use std::any::type_name;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::ops::AddAssign;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, Result};
use arrow::csv;
use arrow::datatypes::{DataType, FieldRef, SchemaRef};
use parquet::basic::Type;
use parquet::column::reader::get_typed_column_reader;
use parquet::file::reader::{FileReader, SerializedFileReader};
use tabled::settings::object::Columns;
use tabled::settings::{Alignment, Modify, Style};
use tabled::{Table, Tabled};

pub use opt::BenchOpt;
use pco::data_types::CoreDataType;

use crate::bench::codecs::CodecConfig;
use crate::bench::num_vec::NumVec;
use crate::dtypes::PcoNumberLike;
use crate::{arrow_handlers, dtypes, parse};
use codecs::CodecSurface;
use pco::with_core_dtypes;

mod codecs;
pub mod handler;
pub mod num_vec;
mod opt;

const DEFAULT_BINARY_DIR: &str = "data/binary";
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
  dtype: CoreDataType,
}

fn handle(
  num_vec: &NumVec,
  dataset: String,
  config: &CodecConfig,
  opt: &BenchOpt,
) -> Result<PrintStat> {
  let precomputed = config.warmup_iter(num_vec, &dataset, &opt.iter_opt)?;
  let mut benches = Vec::with_capacity(opt.iters);
  for _ in 0..opt.iters {
    benches.push(config.stats_iter(num_vec, &precomputed, &opt.iter_opt));
  }
  Ok(PrintStat::compute(
    dataset,
    config.to_string(),
    &benches,
  ))
}

fn get_name_and_dtype(path: &Path) -> Result<(String, DataType)> {
  let mut split = basename_no_ext(path).split('_');
  let invalid_filename = || {
    anyhow!(
      "filename must be of the format <DTYPE>_<NAME>, but was {:?}",
      path
    )
  };
  let dtype_str = split.next().ok_or_else(invalid_filename)?;
  let dtype = parse::arrow_dtype(&dtype_str)?;
  let name = split.next().ok_or_else(invalid_filename)?.to_string();
  Ok((name, dtype))
}

fn handle_synthetic(path: &Path, config: &CodecConfig, opt: &BenchOpt) -> Result<PrintStat> {
  let (dataset, arrow_dtype) = get_name_and_dtype(path)?;

  let raw_bytes = fs::read(path).expect("could not read");
  let core_dtype = dtypes::from_arrow(&arrow_dtype)?;
  let num_vec = NumVec::new(core_dtype, raw_bytes, opt.limit);
  handle(&num_vec, dataset, config, opt)
}

fn core_dtype_to_str(dtype: CoreDataType) -> String {
  macro_rules! to_str {
    {$($name:ident($lname:ident) => $t:ty,)+} => {
      match dtype {
        $(CoreDataType::$name => type_name::<$t>(),)+
      }
    }
  }

  let name = with_core_dtypes!(to_str);
  name.to_string()
}

fn collect_parquet_num_vec<T: PcoNumberLike>(
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
  opt: &BenchOpt,
) -> Result<Vec<PrintStat>> {
  let pq_meta = pq_reader.metadata();
  let pq_schema = pq_meta.file_metadata().schema_descr();
  let pq_col = pq_schema.column(col_idx);
  let arrow_schema = parquet::arrow::parquet_to_arrow_schema(pq_schema, None)?;
  let arrow_dtype = arrow_schema.fields[col_idx].data_type();
  let num_vec = match pq_col.physical_type() {
    Type::INT32 => collect_parquet_num_vec::<i32>(pq_reader, col_idx, n),
    Type::INT64 => collect_parquet_num_vec::<i64>(pq_reader, col_idx, n),
    Type::FLOAT => collect_parquet_num_vec::<f32>(pq_reader, col_idx, n),
    Type::DOUBLE => collect_parquet_num_vec::<f64>(pq_reader, col_idx, n),
    _ => return Ok(vec![]),
  };

  let dtype = num_vec.dtype();
  let dataset = format!(
    "{}_{}",
    core_dtype_to_str(dtype),
    pq_col.name()
  );
  if !opt.includes_dtype(arrow_dtype) || !opt.includes_dataset(&dataset) {
    return Ok(vec![]);
  }

  let mut stats = Vec::new();
  for codec in &opt.codecs {
    stats.push(handle(
      &num_vec,
      dataset.to_string(),
      codec,
      opt,
    )?);
  }

  Ok(stats)
}

fn handle_csv_column(
  path: &Path,
  field_idx: usize,
  schema: SchemaRef,
  opt: &BenchOpt,
) -> Result<Vec<PrintStat>> {
  let field = &schema.fields[field_idx];
  if !opt.dtypes.contains(field.data_type()) || !opt.includes_dataset(field.name()) {
    return Ok(vec![]);
  }

  let mut csv_reader = csv::ReaderBuilder::new(schema)
    .with_header(opt.input.csv_has_header)
    .with_delimiter(opt.input.csv_delimiter as u8)
    .build(File::open(path)?)?;
  let mut arrow_arrays = Vec::new();
  for batch in &mut csv_reader {
    let batch = batch?;
    arrow_arrays.push(batch.column(field_idx).clone());
  }
  let handler = arrow_handlers::from_dtype(field.data_type())?;
  handler.bench_from_arrow(&arrow_arrays, field.name(), opt)
}

fn handle_binary(dir: &Path, opt: &BenchOpt) -> Result<Vec<PrintStat>> {
  let mut paths = Vec::new();
  for f in fs::read_dir(dir)? {
    let path = f?.path();
    let (dataset, dtype) = get_name_and_dtype(&path)?;
    if opt.includes_dtype(&dtype) && opt.includes_dataset(&dataset) {
      paths.push(path);
    }
  }
  paths.sort();

  let mut stats = Vec::new();
  for path in paths {
    for config in &opt.codecs {
      stats.push(handle_synthetic(&path, config, &opt)?);
    }
  }
  Ok(stats)
}

fn handle_parquet(path: &Path, opt: &BenchOpt) -> Result<Vec<PrintStat>> {
  let file = File::open(path)?;
  let pq_reader = SerializedFileReader::new(file)?;
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
    )?);
  }
  Ok(stats)
}

fn handle_csv(path: &Path, opt: &BenchOpt) -> Result<Vec<PrintStat>> {
  let input = &opt.input;
  let schema = csv::infer_schema_from_files(
    &[path.to_str().unwrap().to_string()],
    input.csv_delimiter as u8,
    None,
    input.csv_has_header,
  )?;
  let schema_ref = SchemaRef::new(schema);

  let mut stats = Vec::new();
  for field_idx in 0..schema.fields.len() {
    stats.extend(handle_csv_column(
      path,
      field_idx,
      schema_ref.clone(),
      opt,
    )?);
  }
  Ok(stats)
}

fn print_stats(mut stats: Vec<PrintStat>, opt: &BenchOpt) {
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

pub fn bench(opt: BenchOpt) -> Result<()> {
  let stats = match (
    opt.binary_dir,
    opt.input.parquet_path,
    opt.input.csv_path,
  ) {
    (None, None, None) => handle_binary(
      &PathBuf::from(DEFAULT_BINARY_DIR.to_string()),
      &opt,
    ),
    (Some(dir), None, None) => handle_binary(&dir, &opt),
    (None, Some(file), None) => handle_parquet(&file, &opt),
    (None, None, Some(file)) => handle_csv(&file, &opt),
    _ => Err(anyhow!(
      "cannot use more than 1 of binary_dir, csv, and parquet inputs at once"
    )),
  }?;

  print_stats(stats, &opt);

  Ok(())
}
