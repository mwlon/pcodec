#![allow(clippy::useless_transmute)]

mod opt;
mod codec_config;

use std::fs;
use std::io::ErrorKind;
use std::ops::AddAssign;
use std::path::Path;
use std::time::{Duration, Instant};

use clap::Parser;
use tabled::settings::object::Columns;
use tabled::settings::{Alignment, Modify, Style};
use tabled::{Table, Tabled};

use opt::Opt;
use codec_config::CodecConfig;
use pco::data_types::NumberLike as DNumberLike;
use q_compress::data_types::{NumberLike as QNumberLike, TimestampMicros};
use crate::opt::AUTO_DELTA;

const BASE_DIR: &str = "bench/data";
// if this delta order is specified, use a dataset-specific order

trait NumberLike: QNumberLike {
  type Pco: DNumberLike;

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
struct BenchStat {
  pub dataset: String,
  pub codec: String,
  pub compress_dt: Duration,
  pub decompress_dt: Duration,
  pub compressed_size: usize,
  pub iters: usize,
}

#[derive(Tabled)]
struct PrintStat {
  pub dataset: String,
  pub codec: String,
  pub compress_dt: String,
  pub decompress_dt: String,
  pub compressed_size: usize,
}

impl AddAssign for BenchStat {
  fn add_assign(&mut self, rhs: Self) {
    self.compressed_size += rhs.compressed_size;
    self.compress_dt += rhs.compress_dt;
    self.decompress_dt += rhs.decompress_dt;
    self.iters += rhs.iters;
  }
}

impl BenchStat {
  fn normalize(&mut self) {
    self.compressed_size /= self.iters;
    self.compress_dt /= self.iters as u32;
    self.decompress_dt /= self.iters as u32;
    self.iters = 1;
  }
}

impl From<BenchStat> for PrintStat {
  fn from(value: BenchStat) -> Self {
    PrintStat {
      dataset: value.dataset,
      codec: value.codec,
      compressed_size: value.compressed_size,
      compress_dt: format!("{:?}", value.compress_dt),
      decompress_dt: format!("{:?}", value.decompress_dt),
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

struct Precomputed<T: NumberLike> {
  raw_bytes: Vec<u8>,
  nums: Vec<T>,
  compressed: Vec<u8>,
  codec: String,
}

fn cast_to_nums<T: NumberLike>(bytes: Vec<u8>) -> Vec<T> {
  // Here we're assuming the bytes are in the right format for our data type.
  // For instance, chunks of 8 little-endian bytes on most platforms for
  // i64's.
  // This is fast and should work across platforms.
  let n = bytes.len() / (T::PHYSICAL_BITS / 8);
  unsafe {
    let mut nums = std::mem::transmute::<_, Vec<T>>(bytes);
    nums.set_len(n);
    nums
  }
}

fn compress_pco<T: DNumberLike>(nums: &[T], config: pco::CompressorConfig) -> Vec<u8> {
  pco::standalone::simple_compress(config, nums)
}

fn decompress_pco<T: NumberLike>(bytes: &[u8]) -> Vec<T> {
  let v = pco::standalone::auto_decompress::<T::Pco>(bytes).expect("could not decompress");
  T::vec_from_pco(v)
}

fn compress_qco<T: NumberLike>(nums: &[T], config: q_compress::CompressorConfig) -> Vec<u8> {
  q_compress::Compressor::<T>::from_config(config).simple_compress(nums)
}

fn decompress_qco<T: NumberLike>(bytes: &[u8]) -> Vec<T> {
  q_compress::auto_decompress(bytes).expect("could not decompress")
}

fn compress<T: NumberLike>(
  raw_bytes: &[u8],
  nums: &[T],
  config: &CodecConfig,
) -> (Duration, CodecConfig, Vec<u8>) {
  let t = Instant::now();
  let mut qualified_config = config.clone();
  let compressed = match &mut qualified_config {
    CodecConfig::Pco(pco_conf) => {
      let mut conf = pco_conf.clone();
      let pco_nums = T::slice_to_pco(nums);
      if conf.delta_encoding_order == AUTO_DELTA {
        conf.delta_encoding_order =
          pco::auto_compressor_config(pco_nums, conf.compression_level).delta_encoding_order;
      }
      *pco_conf = conf.clone();
      compress_pco(pco_nums, conf)
    }
    CodecConfig::QCompress(qco_conf) => {
      let mut conf = qco_conf.clone();
      if conf.delta_encoding_order == AUTO_DELTA {
        conf.delta_encoding_order =
          q_compress::auto_compressor_config(nums, conf.compression_level).delta_encoding_order;
      }
      *qco_conf = conf.clone();
      compress_qco(nums, conf)
    }
    CodecConfig::ZStd(level) => {
      let level = *level as i32;
      zstd::encode_all(raw_bytes, level).unwrap()
    }
  };
  (
    Instant::now() - t,
    qualified_config,
    compressed,
  )
}

fn decompress<T: NumberLike>(
  compressed: &[u8],
  config: &CodecConfig,
) -> (Duration, Vec<T>) {
  let t = Instant::now();
  let rec_nums = match config {
    CodecConfig::Pco(_) => decompress_pco(compressed),
    CodecConfig::QCompress(_) => decompress_qco(compressed),
    CodecConfig::ZStd(_) => {
      // to do justice to zstd, unsafely convert the bytes it writes into T
      // without copying
      let decoded_bytes = zstd::decode_all(compressed).unwrap();
      cast_to_nums(decoded_bytes)
    }
  };
  (Instant::now() - t, rec_nums)
}

fn warmup_iter<T: NumberLike>(
  path: &Path,
  dataset: &str,
  config: &CodecConfig,
  opt: &Opt,
) -> Precomputed<T> {
  // read in data
  let raw_bytes = fs::read(path).expect("could not read");
  let nums = cast_to_nums(raw_bytes.clone());

  // compress
  let (_, qualified_config, compressed) = compress(&raw_bytes, &nums, config);
  println!(
    "\ndataset warmup: {} config: {:?}",
    dataset, qualified_config
  );
  println!("\tcompressed to {} bytes", compressed.len());

  // write to disk
  let mut fname = dataset.to_string();
  fname.push('_');
  fname.push_str(&qualified_config.details());
  let output_dir = format!("{}/{}", BASE_DIR, config.codec());
  let output_path = format!("{}/{}.qco", output_dir, fname);

  match fs::create_dir(&output_dir) {
    Ok(()) => (),
    Err(e) => match e.kind() {
      ErrorKind::AlreadyExists => (),
      _ => panic!("{}", e),
    },
  }
  fs::write(output_path, &compressed).expect("couldn't write");

  // decompress
  let (_, rec_nums) = decompress::<T>(&compressed, config);

  if !opt.no_assertions {
    // make sure everything came back correct
    if rec_nums.len() != nums.len() {
      println!(
        "original len: {} recovered len: {}",
        nums.len(),
        rec_nums.len()
      );
      panic!("got back the wrong number of numbers!");
    }
    for i in 0..rec_nums.len() {
      if !rec_nums[i].num_eq(&nums[i]) {
        println!("{} num {} -> {}", i, nums[i], rec_nums[i]);
        panic!("failed to recover nums by compressing and decompressing!");
      }
    }
  }

  Precomputed {
    raw_bytes,
    nums,
    compressed,
    codec: qualified_config.to_string(),
  }
}

fn stats_iter<T: NumberLike>(
  dataset: String,
  config: &CodecConfig,
  precomputed: &Precomputed<T>,
  opt: &Opt,
) -> BenchStat {
  // compress
  let compress_dt = if !opt.no_compress {
    let (dt, _, _) = compress(
      &precomputed.raw_bytes,
      &precomputed.nums,
      config,
    );
    println!("\tcompressed in {:?}", dt);
    dt
  } else {
    Duration::ZERO
  };

  // decompress
  let decompress_dt = if !opt.no_decompress {
    let (dt, _) = decompress::<T>(&precomputed.compressed, config);
    println!("\tdecompressed in {:?}", dt);
    dt
  } else {
    Duration::ZERO
  };

  BenchStat {
    dataset,
    codec: precomputed.codec.clone(),
    compressed_size: precomputed.compressed.len(),
    compress_dt,
    decompress_dt,
    iters: 1,
  }
}

fn handle<T: NumberLike>(path: &Path, config: &CodecConfig, opt: &Opt) -> BenchStat {
  let dataset = basename_no_ext(path);

  let precomputed = warmup_iter::<T>(path, &dataset, config, opt);
  let mut full_stat = BenchStat {
    codec: config.codec().to_string(),
    dataset: dataset.clone(),
    ..Default::default()
  };
  for _ in 0..opt.iters {
    let iter_stat = stats_iter::<T>(dataset.clone(), config, &precomputed, opt);
    full_stat.codec = iter_stat.codec.clone(); // sometimes we get a more precise codec name
    full_stat += iter_stat;
  }
  full_stat.normalize();
  full_stat
}

fn print_stats(stats: &[BenchStat]) {
  let mut print_stats = stats
    .iter()
    .cloned()
    .map(PrintStat::from)
    .collect::<Vec<_>>();
  let mut aggregate = BenchStat::default();
  for stat in stats {
    aggregate += stat.clone();
  }
  print_stats.push(PrintStat::from(aggregate));
  let table = Table::new(print_stats)
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
    let keep = opt.datasets.is_empty() || opt.datasets
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

  print_stats(&stats);
}
