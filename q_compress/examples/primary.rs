use std::convert::TryInto;
use std::fmt::{Display, Formatter};
use std::fs;
use std::io::ErrorKind;
use std::ops::AddAssign;
use std::path::Path;
use std::time::{Duration, Instant};

use q_compress::{Compressor, CompressorConfig, Decompressor};
use q_compress::data_types::{NumberLike, TimestampMicros};
use structopt::StructOpt;

const BASE_DIR: &str = "q_compress/examples/data";
// if this delta order is specified, use a dataset-specific order
const MAGIC_DELTA_ORDER: usize = 8;
const SPECIAL_DELTA_ORDERS: [(&str, usize); 3] = [
  ("f64_slow_cosine", 7),
  ("i64_slow_cosine", 2),
  ("micros_near_linear", 1),
];

#[derive(StructOpt)]
struct Opt {
  #[structopt(long, short, default_value="all")]
  datasets: String,
  #[structopt(long, short, default_value="10")]
  pub iters: usize,
  #[structopt(long, short, default_value="qco")]
  compressors: String,
}

#[derive(Clone, Debug)]
enum MultiCompressorConfig {
  QCompress(CompressorConfig),
  ZStd(usize),
}

impl MultiCompressorConfig {
  pub fn codec(&self) -> &'static str {
    match self {
      MultiCompressorConfig::QCompress(_) => "qco",
      MultiCompressorConfig::ZStd(_) => "zstd",
    }
  }

  pub fn details(&self) -> String {
    match self {
      MultiCompressorConfig::QCompress(config) => {
        format!(
          "{}:{}:{}",
          config.compression_level,
          config.delta_encoding_order,
          config.use_gcds
        )
      }
      MultiCompressorConfig::ZStd(level) => {
        format!(
          "{}",
          level,
        )
      }
    }
  }
}

impl Display for MultiCompressorConfig {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "{}:{}",
      self.codec(),
      self.details(),
    )
  }
}

impl Opt {
  pub fn get_datasets(&self) -> Vec<String> {
    let d = self.datasets.to_lowercase();
    d.split(',').map(|s| s.to_string()).collect::<Vec<_>>()
  }

  pub fn get_compressors(&self) -> Vec<MultiCompressorConfig> {
    let mut res = Vec::new();
    for s in self.compressors.to_lowercase().split(',') {
      let parts = s.split(':').collect::<Vec<_>>();
      let level = if parts.len() > 1 {
        Some(parts[1].parse().unwrap())
      } else {
        None
      };
      res.push(match parts[0] {
        "q" | "qco" | "q_compress" => {
          let delta_encoding_order = if parts.len() > 2 {
            parts[2].parse().unwrap()
          } else {
            MAGIC_DELTA_ORDER
          };
          let use_gcds = !(parts.len() > 3 && &parts[3].to_lowercase()[0..3] == "off");
          let config = CompressorConfig::default()
            .with_compression_level(level.unwrap_or(6))
            .with_delta_encoding_order(delta_encoding_order)
            .with_use_gcds(use_gcds);
          MultiCompressorConfig::QCompress(config)
        },
        "zstd" => {
          MultiCompressorConfig::ZStd(level.unwrap_or(3))
        },
        _ => panic!("unknown compressor")
      })
    }
    res
  }
}

struct BenchStat {
  pub dataset: String,
  pub config: MultiCompressorConfig,
  pub compress_dt: Duration,
  pub decompress_dt: Duration,
  pub compressed_size: usize,
  pub iters: usize,
}

impl AddAssign for BenchStat {
  fn add_assign(&mut self, rhs: Self) {
    if self.compressed_size == 0 {
      self.compressed_size = rhs.compressed_size;
    }
    self.compress_dt += rhs.compress_dt;
    self.decompress_dt += rhs.decompress_dt;
    self.iters += rhs.iters;
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

fn delta_order_for_dataset(dataset: &str) -> usize {
  for (name, order) in &SPECIAL_DELTA_ORDERS {
    if dataset.contains(name) {
      return *order;
    }
  }
  0
}

trait DtypeHandler<T: 'static> where T: NumberLike {
  fn parse_nums(bytes: &[u8]) -> Vec<T>;

  fn compress_qco(nums: Vec<T>, config: CompressorConfig) -> Vec<u8> {
    Compressor::<T>::from_config(config)
      .simple_compress(&nums)
  }

  fn decompress_qco(bytes: &[u8]) -> Vec<T> {
    Decompressor::<T>::default()
      .simple_decompress(bytes)
      .expect("could not decompress")
  }

  fn handle(
    path: &Path,
    mut config: MultiCompressorConfig,
    warmup: bool,
  ) -> BenchStat {
    let dataset = basename_no_ext(path);
    println!("\ndataset: {} config: {:?}", dataset, config);

    // compress
    let bytes = fs::read(path).expect("could not read");
    let nums = Self::parse_nums(&bytes);

    let (compressed, compress_dt) = match &mut config {
      MultiCompressorConfig::QCompress(qco_conf) => {
        if qco_conf.delta_encoding_order == MAGIC_DELTA_ORDER {
          qco_conf.delta_encoding_order = delta_order_for_dataset(&dataset);
        }
        let compress_start = Instant::now();
        let compressed = Self::compress_qco(nums.clone(), qco_conf.clone());
        (compressed, Instant::now() - compress_start)
      }
      MultiCompressorConfig::ZStd(level) => {
        let compress_start = Instant::now();
        let compressed = zstd::encode_all(bytes.as_slice(), *level as i32).unwrap();
        (compressed, Instant::now() - compress_start)
      }
    };
    println!("\tcompressed to {} bytes in {:?}", compressed.len(), compress_dt);
    let mut fname = dataset.clone();
    fname.push('_');
    fname.push_str(&config.details());
    let output_dir = format!("{}/{}", BASE_DIR, config.codec());
    let output_path = format!("{}/{}.qco", output_dir, fname);

    if warmup {
      match fs::create_dir(&output_dir) {
        Ok(()) => (),
        Err(e) => match e.kind() {
          ErrorKind::AlreadyExists => (),
          _ => panic!("{}", e)
        }
      }
      fs::write(
        &output_path,
        &compressed,
      ).expect("couldn't write");
    }

    // decompress
    let decompress_start = Instant::now();
    let decompress_dt = match config {
      MultiCompressorConfig::QCompress(_) => {
        let decompress_start = Instant::now();
        let rec_nums = Self::decompress_qco(&compressed);
        let decompress_dt = Instant::now() - decompress_start;

        // make sure everything came back correct
        if rec_nums.len() != nums.len() {
          println!("original len: {} recovered len: {}", nums.len(), rec_nums.len());
          panic!("got back the wrong number of numbers!");
        }
        for i in 0..rec_nums.len() {
          if !rec_nums[i].num_eq(&nums[i]) {
            println!(
              "{} num {} -> {}",
              i,
              nums[i],
              rec_nums[i]
            );
            panic!("failed to recover nums by compressing and decompressing!");
          }
        }

        decompress_dt
      }
      MultiCompressorConfig::ZStd(_) => {
        let decompress_start = Instant::now();
        zstd::decode_all(compressed.as_slice()).unwrap();
        Instant::now() - decompress_start
      }
    };
    println!("\tdecompressed in {:?}", Instant::now() - decompress_start);

    BenchStat {
      dataset,
      config,
      compressed_size: compressed.len(),
      compress_dt,
      decompress_dt,
      iters: 1,
    }
  }
}

struct I64Handler;

impl DtypeHandler<i64> for I64Handler {
  fn parse_nums(bytes: &[u8]) -> Vec<i64> {
    bytes
      .chunks(8)
      // apparently numpy writes in le order
      .map(|chunk| i64::from_le_bytes(chunk.try_into().expect("incorrect # of bytes in file")))
      .collect()
  }
}

struct F64Handler;

impl DtypeHandler<f64> for F64Handler {
  fn parse_nums(bytes: &[u8]) -> Vec<f64> {
    bytes
      .chunks(8)
      .map(|chunk| f64::from_le_bytes(chunk.try_into().expect("incorrect # of bytes in file")))
      .collect()
  }
}

struct BoolHandler;

impl DtypeHandler<bool> for BoolHandler {
  fn parse_nums(bytes: &[u8]) -> Vec<bool> {
    bytes
      .chunks(1)
      .map(|chunk| u8::from_le_bytes(chunk.try_into().expect("incorrect # of bytes in file")) != 0)
      .collect()
  }
}

struct TimestampMicrosHandler;

impl DtypeHandler<TimestampMicros> for TimestampMicrosHandler {
  fn parse_nums(bytes: &[u8]) -> Vec<TimestampMicros> {
    bytes
      .chunks(8)
      // apparently numpy writes in le order
      .map(|chunk| {
        let int = i64::from_le_bytes(chunk.try_into().expect("incorrect # of bytes in file"));
        TimestampMicros::new(int as i128).expect("timestamp creation")
      })
      .collect()
  }
}

fn left_pad(s: &str, size: usize) -> String {
  let mut res = " ".repeat(size.saturating_sub(s.len()));
  res.push_str(s);
  res
}

fn right_pad(s: &str, size: usize) -> String {
  let mut res = s.to_string();
  res.push_str(&" ".repeat(size.saturating_sub(s.len())));
  res
}

fn print_table_line(dataset: &str, compressor: &str, size: &str, compress_dt: &str, decompress_dt: &str) {
  println!(
    "|{}|{}|{}|{}|{}|",
    right_pad(dataset, 20),
    right_pad(compressor, 14),
    left_pad(size, 10),
    left_pad(compress_dt, 13),
    left_pad(decompress_dt, 13),
  );
}

fn print_stats(stats: &[BenchStat]) {
  println!();
  print_table_line("dataset", "compressor", "size", "compress dt", "decompress dt");
  print_table_line("", "", "", "", "");
  for stat in stats {
    print_table_line(
      &stat.dataset,
      &stat.config.to_string(),
      &format!("{}", stat.compressed_size),
      &format!("{:?}", stat.compress_dt / stat.iters as u32),
      &format!("{:?}", stat.decompress_dt / stat.iters as u32),
    );
  }
}

fn main() {
  let opt: Opt = Opt::from_args();

  let files = fs::read_dir(format!("{}/binary", BASE_DIR)).expect("couldn't read");
  let configs = opt.get_compressors();
  let datasets = opt.get_datasets();

  let mut stats = Vec::new();
  for file in files {
    let path = file.unwrap().path();
    let path_str = path.to_str().unwrap();
    let mut skip = true;
    for dataset in &datasets {
      if dataset == "all" || path_str.contains(dataset) {
        skip = false;
        break;
      }
    }
    if skip {
      continue;
    }

    for config in &configs {
      let mut full_stat = None;
      for i in 0..opt.iters + 1 {
        let config = config.clone();
        let warmup = i == 0;

        let iter_stat = if path_str.contains("i64") {
          I64Handler::handle(&path, config, warmup)
        } else if path_str.contains("f64") {
          F64Handler::handle(&path, config, warmup)
        } else if path_str.contains("bool") {
          BoolHandler::handle(&path, config, warmup)
        } else if path_str.contains("micros") {
          TimestampMicrosHandler::handle(&path, config, warmup)
        } else {
          panic!("Could not determine dtype for file {}!", path_str);
        };

        if !warmup {
          if let Some(stat) = &mut full_stat {
            *stat += iter_stat;
          } else {
            full_stat = Some(iter_stat);
          }
        }
      }
      stats.push(full_stat.unwrap());
    }
  }

  print_stats(&stats);
}
