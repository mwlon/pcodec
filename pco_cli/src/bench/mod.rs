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
use arrow::datatypes::{DataType, SchemaRef};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ProjectionMask;
use tabled::settings::object::Columns;
use tabled::settings::{Alignment, Modify, Style};
use tabled::{Table, Tabled};

pub use opt::BenchOpt;
use pco::data_types::CoreDataType;
use pco::with_core_dtypes;

use crate::num_vec::NumVec;
use crate::{arrow_handlers, dtypes, parse};

mod codecs;
pub mod handler;
mod opt;

const DEFAULT_BINARY_DIR: &str = "data/binary";
// if this delta order is specified, use a dataset-specific order

#[derive(Clone, Default)]
pub struct BenchStat {
  pub compress_dt: Duration,
  pub decompress_dt: Duration,
  pub compressed_size: usize,
}

pub struct Precomputed {
  compressed: Vec<u8>,
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

#[derive(Clone, Tabled)]
pub struct PrintStat {
  pub dataset: String,
  pub codec: String,
  #[tabled(display_with = "display_duration")]
  pub compress_dt: Duration,
  #[tabled(display_with = "display_duration")]
  pub decompress_dt: Duration,
  pub compressed_size: usize,
}

impl Default for PrintStat {
  fn default() -> Self {
    Self {
      dataset: "<sum>".to_string(),
      codec: "<sum>".to_string(),
      compress_dt: Duration::default(),
      decompress_dt: Duration::default(),
      compressed_size: 0,
    }
  }
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

fn get_dtype_and_name(path: &Path) -> Result<(DataType, String)> {
  let no_ext = basename_no_ext(path);
  let mut split = no_ext.split('_');
  let invalid_filename = || {
    anyhow!(
      "filename must be of the format <DTYPE>_<NAME>, but was {:?}",
      path
    )
  };
  let dtype_str = split.next().ok_or_else(invalid_filename)?;
  let dtype = parse::arrow_dtype(dtype_str)?;
  let name = split.collect::<Vec<_>>().join("_");
  Ok((dtype, name))
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

fn handle_parquet_column(file: File, col_idx: usize, opt: &BenchOpt) -> Result<Vec<PrintStat>> {
  let arrow_reader_builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
  let schema = arrow_reader_builder.schema().clone();
  let field = &schema.fields[col_idx];

  if !opt.includes_dataset(field.data_type(), field.name()) {
    return Ok(vec![]);
  }

  let schema_desc = arrow_reader_builder
    .metadata()
    .file_metadata()
    .schema_descr();
  let projection = ProjectionMask::roots(schema_desc, vec![col_idx]);
  let reader = arrow_reader_builder.with_projection(projection).build()?;

  let mut arrays = Vec::new();
  for batch_res in reader {
    let batch = batch_res?;
    arrays.push(batch.columns()[0].clone());
  }

  let handler = arrow_handlers::from_dtype(field.data_type())?;
  handler.bench_from_arrow(&arrays, field.name(), opt)
}

fn handle_csv_column(
  path: &Path,
  field_idx: usize,
  schema: SchemaRef,
  opt: &BenchOpt,
) -> Result<Vec<PrintStat>> {
  let field = &schema.fields[field_idx];
  if !opt.includes_dataset(field.data_type(), field.name()) {
    return Ok(vec![]);
  }

  let mut csv_reader = csv::ReaderBuilder::new(schema.clone())
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
    let (dtype, name) = get_dtype_and_name(&path)?;
    if opt.includes_dataset(&dtype, &name) {
      paths.push(path);
    }
  }
  paths.sort();

  let mut stats = Vec::new();
  for path in paths {
    let (dtype, name) = get_dtype_and_name(&path)?;
    let handler = arrow_handlers::from_dtype(&dtype)?;

    let raw_bytes = fs::read(path).expect("could not read");
    let core_dtype = dtypes::from_arrow(&dtype)?;
    let num_vec = NumVec::new(core_dtype, raw_bytes, opt.limit);
    stats.extend(handler.bench(&num_vec, &name, opt)?);
  }
  Ok(stats)
}

fn handle_parquet(path: &Path, opt: &BenchOpt) -> Result<Vec<PrintStat>> {
  let file = File::open(path)?;
  let schema = ArrowReaderMetadata::load(&file, Default::default())?
    .schema()
    .clone();
  let n_cols = schema.fields.len();
  drop(file);

  let mut stats = Vec::new();
  for col_idx in 0..n_cols {
    stats.extend(handle_parquet_column(
      File::open(path)?,
      col_idx,
      opt,
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
  for field_idx in 0..schema_ref.fields.len() {
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
    &opt.binary_dir,
    &opt.input.parquet_path,
    &opt.input.csv_path,
  ) {
    (None, None, None) => handle_binary(
      &PathBuf::from(DEFAULT_BINARY_DIR.to_string()),
      &opt,
    ),
    (Some(dir), None, None) => handle_binary(dir, &opt),
    (None, Some(file), None) => handle_parquet(file, &opt),
    (None, None, Some(file)) => handle_csv(file, &opt),
    _ => Err(anyhow!(
      "cannot use more than 1 of binary_dir, csv, and parquet inputs at once"
    )),
  }?;

  print_stats(stats, &opt);

  Ok(())
}
