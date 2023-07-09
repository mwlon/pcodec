#![allow(clippy::useless_transmute)]

mod codecs;
mod opt;

use std::fs;

use std::path::Path;
use std::time::Duration;

use clap::Parser;
use tabled::settings::object::Columns;
use tabled::settings::{Alignment, Modify, Style};
use tabled::{Table, Tabled};

use crate::codecs::CodecConfig;
use opt::Opt;
use pco::data_types::NumberLike as PNumberLike;
use q_compress::data_types::{NumberLike as QNumberLike, TimestampMicros};

const BASE_DIR: &str = "bench/data";
// if this delta order is specified, use a dataset-specific order

trait NumberLike: QNumberLike {
  type Pco: PNumberLike;

  fn slice_to_pco(slice: &[Self]) -> &[Self::Pco];
  fn vec_from_pco(v: Vec<Self::Pco>) -> Vec<Self>;
}

macro_rules! impl_pco_number_like {
  ($t: ty, $pco: ty) => {
    impl NumberLike for $t {
      type Pco = $pco;

      fn slice_to_pco(slice: &[$t]) -> &[Self::Pco] {
        unsafe { std::mem::transmute(slice) }
      }

      fn vec_from_pco(v: Vec<Self::Pco>) -> Vec<Self> {
        unsafe { std::mem::transmute(v) }
      }
    }
  };
}

impl_pco_number_like!(i64, i64);
impl_pco_number_like!(f32, f32);
impl_pco_number_like!(f64, f64);
impl_pco_number_like!(TimestampMicros, i64);

#[derive(Clone, Default)]
pub struct BenchStat {
  pub compress_dt: Duration,
  pub decompress_dt: Duration,
  pub compressed_size: usize,
}

fn median_duration(mut durations: Vec<Duration>) -> Duration {
  durations.sort_unstable();
  let lo = durations[durations.len() / 2];
  let hi = durations[(durations.len() + 1) / 2];
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
  raw_bytes: Vec<u8>,
  compressed: Vec<u8>,
}

// fn compress_pco<T: DNumberLike>(nums: &[T], config: pco::CompressorConfig) -> Vec<u8> {
//   pco::standalone::simple_compress(config, nums)
// }
//
// fn compress_qco<T: NumberLike>(nums: &[T], config: q_compress::CompressorConfig) -> Vec<u8> {
//   q_compress::Compressor::<T>::from_config(config).simple_compress(nums)
// }
//
// fn decompress_qco<T: NumberLike>(bytes: &[u8]) -> Vec<T> {
//   q_compress::auto_decompress(bytes).expect("could not decompress")
// }
//
// fn compress<T: NumberLike>(
//   nums: &[T],
//   config: &CodecConfig,
// ) -> (Duration, Vec<u8>) {
//   let t = Instant::now();
//   let compressed = config.inner.compress(nums);
//   // let compressed = match &mut qualified_config {
//   //   CodecConfig::Pco(pco_conf) => {
//   //     let mut conf = pco_conf.clone();
//   //     let pco_nums = T::slice_to_pco(nums);
//   //     if conf.delta_encoding_order == AUTO_DELTA {
//   //       conf.delta_encoding_order =
//   //         pco::auto_compressor_config(pco_nums, conf.compression_level).delta_encoding_order;
//   //     }
//   //     *pco_conf = conf.clone();
//   //     compress_pco(pco_nums, conf)
//   //   }
//   //   CodecConfig::QCompress(qco_conf) => {
//   //     let mut conf = qco_conf.clone();
//   //     if conf.delta_encoding_order == AUTO_DELTA {
//   //       conf.delta_encoding_order =
//   //         q_compress::auto_compressor_config(nums, conf.compression_level).delta_encoding_order;
//   //     }
//   //     *qco_conf = conf.clone();
//   //     compress_qco(nums, conf)
//   //   }
//   //   CodecConfig::ZStd(level) => {
//   //     let level = *level as i32;
//   //     zstd::encode_all(raw_bytes, level).unwrap()
//   //   }
//   // };
//   (
//     Instant::now() - t,
//     compressed,
//   )
// }

// fn decompress<T: NumberLike>(
//   compressed: &[u8],
//   config: &CodecConfig,
// ) -> (Duration, Vec<T>) {
//   let t = Instant::now();
//   let rec_nums = config.inner.decompress(compressed);
//   // let rec_nums = match config {
//   //   CodecConfig::Pco(_) => decompress_pco(compressed),
//   //   CodecConfig::QCompress(_) => decompress_qco(compressed),
//   //   CodecConfig::ZStd(_) => {
//   //     // to do justice to zstd, unsafely convert the bytes it writes into T
//   //     // without copying
//   //     let decoded_bytes = zstd::decode_all(compressed).unwrap();
//   //     cast_to_nums(decoded_bytes)
//   //   }
//   // };
//   (Instant::now() - t, rec_nums)
// }

fn handle<T: NumberLike>(path: &Path, config: &CodecConfig, opt: &Opt) -> PrintStat {
  let dataset = basename_no_ext(path);

  let mut fname = dataset.to_string();
  fname.push('_');
  fname.push_str(&config.details());
  let precomputed = config
    .inner
    .warmup_iter(path, &dataset, &fname, &opt.handler_opt);
  let mut benches = Vec::with_capacity(opt.iters);
  for _ in 0..opt.iters {
    benches.push(
      config
        .inner
        .stats_iter(&dataset, &precomputed, &opt.handler_opt),
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
      let stat = if path_str.contains("i64") || path_str.contains("micros") {
        handle::<i64>(&path, config, &opt)
      } else if path_str.contains("f64") {
        handle::<f64>(&path, config, &opt)
      } else if path_str.contains("f32") {
        handle::<f32>(&path, config, &opt)
      } else if path_str.contains("micros") {
        handle::<TimestampMicros>(&path, config, &opt)
      } else {
        panic!(
          "Could not determine dtype for file {}!",
          path_str
        );
      };
      stats.push(stat);
    }
  }

  print_stats(stats);
}
