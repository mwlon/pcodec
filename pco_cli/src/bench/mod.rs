#![allow(clippy::uninit_vec)]

use std::any::type_name;
use std::collections::HashMap;
use std::ops::AddAssign;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use anyhow::Result;
use arrow::datatypes::{DataType, Schema};
use clap::{Args, Parser};
use indicatif::{ProgressBar, ProgressStyle};
use tabled::settings::object::Columns;
use tabled::settings::{Alignment, Modify, Style};
use tabled::{Table, Tabled};

use pco::data_types::CoreDataType;
use pco::with_core_dtypes;

use crate::bench::codecs::CodecConfig;
use crate::input::{InputColumnOpt, InputFileOpt};
use crate::{arrow_handlers, dtypes, input, parse};

mod codecs;
pub mod handler;

const DEFAULT_BINARY_DIR: &str = "data/binary";
// if this delta order is specified, use a dataset-specific order

/// Run benchmarks on datasets originating from another format.
///
/// This supports various input formats, various codecs (add even more with the
/// full_bench cargo feature), and configurations for each codec.
///
/// The input format does not affect performance; all input numbers are
/// loaded into memory prior to benchmarking each dataset.
/// By default, if no inputs are specified, the bench will use the
/// relative directory `data/binary/` as binary input.
#[derive(Clone, Debug, Parser)]
pub struct BenchOpt {
  /// Comma-separated list of codecs to benchmark, optionally with
  /// colon-separated configurations.
  ///
  /// For example, setting this to
  /// `zstd,zstd:level=10,pco:level=9:delta_order=0`
  /// will compare 3 codecs: zstd at default compression level (3), zstd at
  /// level 10, and pco at level 9 with 0th order delta encoding.
  ///
  /// To see what valid configurations look like, try entering an invalid one.
  #[arg(long, short, default_value = "pco", value_parser = CodecConfig::from_str, value_delimiter = ',')]
  pub codecs: Vec<CodecConfig>,
  /// Comma-separated substrings of datasets or column names to benchmark.
  /// By default all datasets are run.
  #[arg(long, short, default_values_t = Vec::<String>::new(), value_delimiter = ',')]
  pub datasets: Vec<String>,
  /// Filter down to datasets or columns matching this Arrow data type,
  /// e.g. i32 or micros.
  #[arg(long, default_values_t = Vec::<DataType>::new(), value_parser = parse::arrow_dtype, value_delimiter = ',')]
  pub dtypes: Vec<DataType>,
  /// Number of iterations to run each codec x dataset combination for
  /// better estimation of durations.
  /// The median duration is kept.
  #[arg(long, short, default_value = "10")]
  pub iters: usize,
  /// How many numbers to limit each dataset to.
  #[arg(long, short)]
  pub limit: Option<usize>,
  #[command(flatten)]
  pub input: InputFileOpt,
  #[command(flatten)]
  pub iter_opt: IterOpt,
}

#[derive(Clone, Debug, Args)]
pub struct IterOpt {
  #[arg(long)]
  pub no_compress: bool,
  #[arg(long)]
  pub no_decompress: bool,
  /// Skip assertions that all the numbers came back bitwise identical.
  ///
  /// This does not affect benchmark timing.
  #[arg(long)]
  pub no_assertions: bool,
  /// Optionally, a directory to save the compressed data to.
  /// Will overwrite conflicting files.
  #[arg(long)]
  pub save_dir: Option<PathBuf>,
}

impl BenchOpt {
  pub fn includes_dataset(&self, dtype: &DataType, name: &str) -> bool {
    if dtypes::from_arrow(dtype).is_err()
      || (!self.dtypes.is_empty() && !self.dtypes.contains(dtype))
    {
      return false;
    }

    self.datasets.is_empty()
      || self
        .datasets
        .iter()
        .any(|allowed_substr| name.contains(allowed_substr))
  }
}

#[derive(Clone, Default)]
pub struct BenchStat {
  pub compress_dt: Duration,
  pub decompress_dt: Duration,
  pub compressed_size: usize,
}

pub struct Precomputed {
  compressed: Vec<u8>,
}

fn make_progress_bar(n_columns: usize, opt: &BenchOpt) -> ProgressBar {
  ProgressBar::new((opt.codecs.len() * n_columns * (opt.iters + 1)) as u64)
    .with_message("iters")
    .with_style(
      ProgressStyle::with_template("[{elapsed_precise}] {wide_bar} {pos}/{len} {msg} ").unwrap(),
    )
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

fn handle_column(
  schema: &Schema,
  col_idx: usize,
  opt: &BenchOpt,
  progress_bar: &mut ProgressBar,
) -> Result<Vec<PrintStat>> {
  let field = &schema.fields[col_idx];
  let reader = input::new_column_reader(schema, col_idx, &opt.input)?;
  let mut arrays = Vec::new();
  for array_result in reader {
    arrays.push(array_result?);
  }
  let handler = arrow_handlers::from_dtype(field.data_type())?;
  handler.bench_from_arrow(&arrays, field.name(), opt, progress_bar)
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

pub fn bench(mut opt: BenchOpt) -> Result<()> {
  let input = &mut opt.input;
  if input.binary_dir.is_none() && input.csv_path.is_none() && input.parquet_path.is_none() {
    input.binary_dir = Some(PathBuf::from(DEFAULT_BINARY_DIR));
  }

  let schema = input::get_schema(&InputColumnOpt::default(), input)?;

  let col_idxs = schema
    .fields
    .iter()
    .enumerate()
    .filter_map(|(i, field)| {
      if opt.includes_dataset(field.data_type(), field.name()) {
        Some(i)
      } else {
        None
      }
    })
    .collect::<Vec<_>>();
  let mut progress_bar = make_progress_bar(col_idxs.len(), &opt);
  let mut stats = Vec::new();
  for col_idx in col_idxs {
    stats.extend(handle_column(
      &schema,
      col_idx,
      &opt,
      &mut progress_bar,
    )?);
  }
  progress_bar.finish_and_clear();

  print_stats(stats, &opt);

  Ok(())
}
