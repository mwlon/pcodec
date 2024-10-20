#![allow(clippy::uninit_vec)]

use std::collections::HashMap;
use std::ops::AddAssign;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use std::{any, fs};

use anyhow::{anyhow, Result};
use arrow::datatypes::{DataType, Schema};
use clap::{Args, Parser};
use indicatif::{ProgressBar, ProgressStyle};
use tabled::settings::object::Columns;
use tabled::settings::{Alignment, Modify, Style};
use tabled::{Table, Tabled};

use pco::data_types::CoreDataType;
use pco::match_number_like_enum;

use crate::bench::codecs::CodecConfig;
use crate::input::{Format, InputColumnOpt, InputFileOpt};
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
  #[arg(long, default_value = "10")]
  pub iters: usize,
  /// How many numbers to limit each dataset to.
  #[arg(long, short)]
  pub limit: Option<usize>,
  /// CSV to write the aggregate results of this command to.
  /// Overwrites any rows with the same input name and codec config.
  /// Columns of output CSV:
  /// input_name, codec, compression_time/s, decompress_time/s, compressed_size/bytes
  #[arg(long)]
  pub results_csv: Option<PathBuf>,
  /// Name of the input data to use in the --results-csv output.
  /// If you're not writing the results to a CSV, ignore this.
  #[arg(long)]
  pub input_name: Option<String>,
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

#[derive(Clone, Default, Tabled)]
pub struct BenchStat {
  #[tabled(display_with = "display_duration")]
  pub compress_dt: Duration,
  #[tabled(display_with = "display_duration")]
  pub decompress_dt: Duration,
  pub compressed_size: usize,
}

#[derive(Clone, Tabled)]
pub struct PrintStat {
  pub dataset: String,
  pub codec: String,
  #[tabled(inline)]
  pub bench_stat: BenchStat,
}

impl AddAssign for BenchStat {
  fn add_assign(&mut self, rhs: Self) {
    self.compressed_size += rhs.compressed_size;
    self.compress_dt += rhs.compress_dt;
    self.decompress_dt += rhs.decompress_dt;
  }
}

impl BenchStat {
  fn aggregate_median(benches: &[BenchStat]) -> Self {
    let compressed_size = benches[0].compressed_size;
    let compress_dts = benches
      .iter()
      .map(|bench| bench.compress_dt)
      .collect::<Vec<_>>();
    let decompress_dts = benches
      .iter()
      .map(|bench| bench.decompress_dt)
      .collect::<Vec<_>>();

    BenchStat {
      compressed_size,
      compress_dt: median_duration(compress_dts),
      decompress_dt: median_duration(decompress_dts),
    }
  }
}

fn type_basename<T>() -> String {
  any::type_name::<T>().split(':').last().unwrap().to_string()
}

fn core_dtype_to_str(dtype: CoreDataType) -> String {
  match_number_like_enum!(
    dtype,
    CoreDataType<T> => {
      type_basename::<T>()
    }
  )
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
  handler.bench(&arrays, field.name(), opt, progress_bar)
}

fn update_results_csv(
  aggregate_by_codec: &HashMap<String, BenchStat>,
  opt: &BenchOpt,
) -> Result<()> {
  // do nothing if the user didn't provide a results CSV
  let Some(results_csv) = opt.results_csv.as_ref() else {
    return Ok(());
  };

  let input_name = opt.input_name.as_ref().unwrap();

  let mut lines = if results_csv.exists() {
    // hacky split on commas, doesn't handle case when values contain weird characters
    let mut lines = HashMap::new();
    let contents = fs::read_to_string(results_csv)?;
    let mut is_header = true;
    for line in contents.split('\n') {
      if is_header {
        is_header = false;
        continue;
      }

      let mut fields = line.split(',');
      let dataset = fields.next();
      let codec = fields.next();
      let (Some(dataset), Some(codec)) = (dataset, codec) else {
        continue;
      };
      let rest = fields.collect::<Vec<_>>().join(",");
      lines.insert(
        (dataset.to_string(), codec.to_string()),
        rest,
      );
    }
    lines
  } else {
    HashMap::new()
  };

  for (codec, stat) in aggregate_by_codec.iter() {
    lines.insert(
      (input_name.to_string(), codec.to_string()),
      format!(
        "{},{},{}",
        stat.compress_dt.as_secs_f32(),
        stat.decompress_dt.as_secs_f32(),
        stat.compressed_size
      ),
    );
  }

  let mut output_lines = vec!["input,codec,compress_dt,decompress_dt,compressed_size".to_string()];
  let mut lines = lines.iter().collect::<Vec<_>>();
  lines.sort_unstable_by_key(|&(key, _)| key);
  for ((dataset, codec), values) in lines {
    output_lines.push(format!("{},{},{}", dataset, codec, values));
  }
  let output = output_lines.join("\n");
  fs::write(results_csv, output)?;

  Ok(())
}

fn print_stats(mut stats: Vec<PrintStat>, opt: &BenchOpt) -> Result<()> {
  if stats.is_empty() {
    return Err(anyhow!(
      "No datasets found that match filters"
    ));
  }

  let mut aggregate = BenchStat::default();
  let mut aggregate_by_codec: HashMap<String, BenchStat> = HashMap::new();
  for stat in &stats {
    aggregate += stat.bench_stat.clone();
    aggregate_by_codec
      .entry(stat.codec.clone())
      .or_default()
      .add_assign(stat.bench_stat.clone());
  }
  stats.extend(opt.codecs.iter().map(|codec| {
    let codec = codec.to_string();
    PrintStat {
      bench_stat: aggregate_by_codec.get(&codec).cloned().unwrap(),
      codec,
      dataset: "<sum>".to_string(),
    }
  }));
  stats.push(PrintStat {
    bench_stat: aggregate,
    codec: "<sum>".to_string(),
    dataset: "<sum>".to_string(),
  });
  let table = Table::new(stats)
    .with(Style::rounded())
    .with(Modify::new(Columns::new(2..)).with(Alignment::right()))
    .to_string();
  println!("{}", table);
  update_results_csv(&aggregate_by_codec, opt)
}

pub fn bench(mut opt: BenchOpt) -> Result<()> {
  if opt.results_csv.is_some() && opt.input_name.is_none() {
    return Err(anyhow!(
      "input-name must be specified when results-csv is"
    ));
  }
  let input = &mut opt.input;
  if input.input.is_none() && input.input_format.is_none() {
    input.input = Some(PathBuf::from(DEFAULT_BINARY_DIR));
    input.input_format = Some(Format::Binary);
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

  print_stats(stats, &opt)
}
