use std::fmt::{Display, Formatter};
use std::fs;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::ops::AddAssign;
use std::path::Path;
use std::time::{Duration, Instant};

use q_compress::{auto_decompress, Compressor, CompressorConfig, DEFAULT_COMPRESSION_LEVEL};
use q_compress::data_types::{NumberLike, TimestampMicros};
use structopt::StructOpt;

const BASE_DIR: &str = "q_compress/examples/data";
// if this delta order is specified, use a dataset-specific order
const MAGIC_DELTA_ORDER: usize = 8;
const SPECIAL_DELTA_ORDERS: [(&str, usize); 4] = [
  ("f64_slow_cosine", 7),
  ("i64_slow_cosine", 2),
  ("micros_near_linear", 1),
  ("interl", 1),
];

#[derive(StructOpt)]
struct Opt {
  #[structopt(long, short, default_value="all")]
  datasets: String,
  #[structopt(long, short, default_value="10")]
  pub iters: usize,
  #[structopt(long, short, default_value="qco")]
  compressors: String,
  #[structopt(long)]
  pub no_compress: bool,
  #[structopt(long)]
  pub no_decompress: bool,
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
            .with_compression_level(level.unwrap_or(DEFAULT_COMPRESSION_LEVEL))
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

struct Precomputed<T: NumberLike> {
  raw_bytes: Vec<u8>,
  nums: Vec<T>,
  compressed: Vec<u8>,
}

struct DtypeHandler<T: 'static + NumberLike>(PhantomData<T>);

impl<T: 'static + NumberLike> DtypeHandler<T> {
  fn cast_to_nums(bytes: Vec<u8>) -> Vec<T> {
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

  fn compress_qco(nums: &[T], config: CompressorConfig) -> Vec<u8> {
    Compressor::<T>::from_config(config)
      .simple_compress(&nums)
  }

  fn decompress_qco(bytes: &[u8]) -> Vec<T> {
    auto_decompress(bytes).expect("could not decompress")
  }

  fn compress(
    dataset: &str,
    raw_bytes: &[u8],
    nums: &[T],
    config: &MultiCompressorConfig,
  ) -> (Duration, Vec<u8>) {
    let t = Instant::now();
    let compressed = match &config {
      MultiCompressorConfig::QCompress(qco_conf) => {
        let mut conf = qco_conf.clone();
        if conf.delta_encoding_order == MAGIC_DELTA_ORDER {
          conf.delta_encoding_order = delta_order_for_dataset(&dataset);
        }
        let compressed = Self::compress_qco(&nums, conf);
        compressed
      }
      MultiCompressorConfig::ZStd(level) => {
        let level = *level as i32;
        zstd::encode_all(raw_bytes, level).unwrap()
      }
    };
    (Instant::now() - t, compressed)
  }

  fn decompress(
    compressed: &[u8],
    config: &MultiCompressorConfig,
  ) -> (Duration, Vec<T>) {
    let t = Instant::now();
    let rec_nums = match config {
      MultiCompressorConfig::QCompress(_) => {
        Self::decompress_qco(compressed)
      }
      MultiCompressorConfig::ZStd(_) => {
        // to do justice to zstd, unsafely convert the bytes it writes into T
        // without copying
        let decoded_bytes = zstd::decode_all(compressed).unwrap();
        Self::cast_to_nums(decoded_bytes)
      }
    };
    (Instant::now() - t, rec_nums)
  }

  fn warmup_iter(
    path: &Path,
    dataset: &str,
    config: &MultiCompressorConfig,
  ) -> Precomputed<T> {
    println!("\ndataset warmup: {} config: {:?}", dataset, config);

    // read in data
    let raw_bytes = fs::read(path).expect("could not read");
    let nums = Self::cast_to_nums(raw_bytes.clone());

    // compress
    let (_, compressed) = Self::compress(dataset, &raw_bytes, &nums, config);
    println!("\tcompressed to {} bytes", compressed.len());

    // write to disk
    let mut fname = dataset.to_string();
    fname.push('_');
    fname.push_str(&config.details());
    let output_dir = format!("{}/{}", BASE_DIR, config.codec());
    let output_path = format!("{}/{}.qco", output_dir, fname);

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

    // decompress
    let (_, rec_nums) = Self::decompress(&compressed, config);

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

    Precomputed {
      raw_bytes,
      nums,
      compressed,
    }
  }

  fn stats_iter(
    dataset: &str,
    config: MultiCompressorConfig,
    precomputed: &Precomputed<T>,
    opt: &Opt,
  ) -> BenchStat {
    // compress
    let compress_dt = if !opt.no_compress {
      let (dt, _) = Self::compress(dataset, &precomputed.raw_bytes, &precomputed.nums, &config);
      println!("\tcompressed in {:?}", dt);
      dt
    } else {
      Duration::ZERO
    };

    // decompress
    let decompress_dt = if !opt.no_decompress {
      let (dt, _) = Self::decompress(&precomputed.compressed, &config);
      println!("\tdecompressed in {:?}", dt);
      dt
    } else {
      Duration::ZERO
    };

    BenchStat {
      dataset: dataset.to_string(),
      config,
      compressed_size: precomputed.compressed.len(),
      compress_dt,
      decompress_dt,
      iters: 1,
    }
  }

  fn handle(
    path: &Path,
    config: &MultiCompressorConfig,
    opt: &Opt,
  ) -> BenchStat {
    let dataset = basename_no_ext(path);
    let precomputed = Self::warmup_iter(path, &dataset, &config);
    let mut full_stat = None;
    for _ in 0..opt.iters {
      let config = config.clone();
      let iter_stat = Self::stats_iter(&dataset, config.clone(), &precomputed, opt);

      if let Some(stat) = &mut full_stat {
        *stat += iter_stat;
      } else {
        full_stat = Some(iter_stat);
      }
    }
    full_stat.unwrap()
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
      let full_stat = if path_str.contains("i64") {
        DtypeHandler::<i64>::handle(&path, config, &opt)
      } else if path_str.contains("f64") {
        DtypeHandler::<f64>::handle(&path, config, &opt)
      } else if path_str.contains("bool") {
        DtypeHandler::<bool>::handle(&path, config, &opt)
      } else if path_str.contains("micros") {
        DtypeHandler::<TimestampMicros>::handle(&path, config, &opt)
      } else {
        panic!("Could not determine dtype for file {}!", path_str);
      };
      stats.push(full_stat);
    }
  }

  print_stats(&stats);
}
