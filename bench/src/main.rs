use std::fmt::{Display, Formatter};
use std::fs;
use std::io::ErrorKind;
use std::ops::AddAssign;
use std::path::Path;
use std::time::{Duration, Instant};

use structopt::StructOpt;

use durendal::data_types::NumberLike as DNumberLike;
use q_compress::data_types::{NumberLike as QNumberLike, TimestampMicros};

const BASE_DIR: &str = "bench/data";
// if this delta order is specified, use a dataset-specific order
const AUTO_DELTA: usize = usize::MAX;

#[derive(StructOpt)]
struct Opt {
  #[structopt(long, short, default_value = "all")]
  datasets: String,
  #[structopt(long, short, default_value = "10")]
  pub iters: usize,
  #[structopt(long, short, default_value = "dur")]
  compressors: String,
  #[structopt(long)]
  pub no_compress: bool,
  #[structopt(long)]
  pub no_decompress: bool,
  #[structopt(long)]
  pub no_assertions: bool,
}

trait NumberLike: QNumberLike{
  type Durendal: DNumberLike;
  const SUPPORTS_DUR: bool;

  fn slice_to_durendal(slice: &[Self]) -> &[Self::Durendal];
  fn vec_from_durendal(v: Vec<Self::Durendal>) -> Vec<Self>;
}

macro_rules! impl_dur_number_like {
  ($t: ty, $dur: ty) => {
    impl NumberLike for $t {
      type Durendal = $dur;
      const SUPPORTS_DUR: bool = true;

      fn slice_to_durendal(slice: &[$t]) -> &[Self::Durendal] {
        unsafe { std::mem::transmute(slice) }
      }

      fn vec_from_durendal(v: Vec<Self::Durendal>) -> Vec<Self> {
        unsafe { std::mem::transmute(v) }
      }
    }
  }
}

macro_rules! impl_non_dur_number_like {
  ($t: ty) => {
    impl NumberLike for $t {
      type Durendal = u32;
      const SUPPORTS_DUR: bool = false;

      fn slice_to_durendal(_slice: &[$t]) -> &[Self::Durendal] {
        panic!()
      }

      fn vec_from_durendal(_v: Vec<Self::Durendal>) -> Vec<Self> {
        panic!()
      }
    }
  }
}

impl_dur_number_like!(i64, i64);
impl_dur_number_like!(f32, f32);
impl_dur_number_like!(f64, f64);
impl_dur_number_like!(TimestampMicros, i64);
impl_non_dur_number_like!(bool);

#[derive(Clone, Debug)]
enum MultiCompressorConfig {
  Durendal(durendal::CompressorConfig),
  QCompress(q_compress::CompressorConfig),
  ZStd(usize),
}

impl MultiCompressorConfig {
  pub fn codec(&self) -> &'static str {
    match self {
      MultiCompressorConfig::Durendal(_) => "dur",
      MultiCompressorConfig::QCompress(_) => "qco",
      MultiCompressorConfig::ZStd(_) => "zstd",
    }
  }

  pub fn details(&self) -> String {
    match self {
      MultiCompressorConfig::Durendal(config) => {
        format!(
          "{}:{}:{}",
          config.compression_level, config.delta_encoding_order, config.use_gcds
        )
      }
      MultiCompressorConfig::QCompress(config) => {
        format!(
          "{}:{}:{}",
          config.compression_level, config.delta_encoding_order, config.use_gcds
        )
      }
      MultiCompressorConfig::ZStd(level) => {
        format!("{}", level,)
      }
    }
  }
}

impl Display for MultiCompressorConfig {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}:{}", self.codec(), self.details(),)
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
        "d" | "dur" | "durendal" => {
          let delta_encoding_order = if parts.len() > 2 {
            parts[2].parse().unwrap()
          } else {
            AUTO_DELTA
          };
          let use_gcds = !(parts.len() > 3 && &parts[3].to_lowercase()[0..3] == "off");
          let config = durendal::CompressorConfig::default()
            .with_compression_level(level.unwrap_or(q_compress::DEFAULT_COMPRESSION_LEVEL))
            .with_delta_encoding_order(delta_encoding_order)
            .with_use_gcds(use_gcds);
          MultiCompressorConfig::Durendal(config)
        }
        "q" | "qco" | "q_compress" => {
          let delta_encoding_order = if parts.len() > 2 {
            parts[2].parse().unwrap()
          } else {
            AUTO_DELTA
          };
          let use_gcds = !(parts.len() > 3 && &parts[3].to_lowercase()[0..3] == "off");
          let config = q_compress::CompressorConfig::default()
            .with_compression_level(level.unwrap_or(q_compress::DEFAULT_COMPRESSION_LEVEL))
            .with_delta_encoding_order(delta_encoding_order)
            .with_use_gcds(use_gcds);
          MultiCompressorConfig::QCompress(config)
        }
        "zstd" => MultiCompressorConfig::ZStd(level.unwrap_or(3)),
        _ => panic!("unknown compressor"),
      })
    }
    res
  }
}

struct BenchStat {
  pub dataset: String,
  pub codec: String,
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

fn compress_dur<T: DNumberLike>(nums: &[T], config: durendal::CompressorConfig) -> Vec<u8> {
  durendal::standalone::simple_compress(config, nums)
}

fn decompress_dur<T: NumberLike>(bytes: &[u8]) -> Vec<T> {
  let v = durendal::standalone::auto_decompress::<T::Durendal>(bytes).expect("could not decompress");
  T::vec_from_durendal(v)
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
  config: &MultiCompressorConfig,
) -> (Duration, MultiCompressorConfig, Vec<u8>) {
  let t = Instant::now();
  let mut qualified_config = config.clone();
  let compressed = match &mut qualified_config {
    MultiCompressorConfig::Durendal(dur_conf) => {
      let mut conf = dur_conf.clone();
      let dur_nums = T::slice_to_durendal(nums);
      if conf.delta_encoding_order == AUTO_DELTA {
        conf.delta_encoding_order =
          durendal::standalone::auto_compressor_config(dur_nums, conf.compression_level).delta_encoding_order;
      }
      *dur_conf = conf.clone();
      compress_dur(dur_nums, conf)
    }
    MultiCompressorConfig::QCompress(qco_conf) => {
      let mut conf = qco_conf.clone();
      if conf.delta_encoding_order == AUTO_DELTA {
        conf.delta_encoding_order =
          q_compress::auto_compressor_config(nums, conf.compression_level).delta_encoding_order;
      }
      *qco_conf = conf.clone();
      compress_qco(nums, conf)
    }
    MultiCompressorConfig::ZStd(level) => {
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
  config: &MultiCompressorConfig,
) -> (Duration, Vec<T>) {
  let t = Instant::now();
  let rec_nums = match config {
    MultiCompressorConfig::Durendal(_) => decompress_dur(compressed),
    MultiCompressorConfig::QCompress(_) => decompress_qco(compressed),
    MultiCompressorConfig::ZStd(_) => {
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
  config: &MultiCompressorConfig,
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
  config: &MultiCompressorConfig,
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

fn handle<T: NumberLike>(path: &Path, config: &MultiCompressorConfig, opt: &Opt) -> Option<BenchStat> {
  let dataset = basename_no_ext(path);
  match config {
    MultiCompressorConfig::Durendal(_) if !T::SUPPORTS_DUR => {
      println!("Skipping {} for Durendal; unsupported dtype", dataset);
      return None;
    }
    _ => ()
  };

  let precomputed = warmup_iter::<T>(path, &dataset, config, opt);
  let mut full_stat = None;
  for _ in 0..opt.iters {
    let iter_stat = stats_iter::<T>(dataset.clone(), config, &precomputed, opt);

    if let Some(stat) = &mut full_stat {
      *stat += iter_stat;
    } else {
      full_stat = Some(iter_stat);
    }
  }
  full_stat
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

fn print_table_line(
  dataset: &str,
  codec: &str,
  size: &str,
  compress_dt: &str,
  decompress_dt: &str,
) {
  println!(
    "|{}|{}|{}|{}|{}|",
    right_pad(dataset, 20),
    right_pad(codec, 14),
    left_pad(size, 10),
    left_pad(compress_dt, 13),
    left_pad(decompress_dt, 13),
  );
}

fn print_stats(stats: &[BenchStat]) {
  println!();
  print_table_line(
    "dataset",
    "compressor",
    "size",
    "compress dt",
    "decompress dt",
  );
  print_table_line("", "", "", "", "");
  for stat in stats {
    print_table_line(
      &stat.dataset,
      &stat.codec,
      &format!("{}", stat.compressed_size),
      &format!("{:?}", stat.compress_dt / stat.iters as u32),
      &format!(
        "{:?}",
        stat.decompress_dt / stat.iters as u32
      ),
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
      let stat = if path_str.contains("i64") || path_str.contains("micros") {
        handle::<i64>(&path, config, &opt)
      } else if path_str.contains("f64") {
        handle::<f64>(&path, config, &opt)
      } else if path_str.contains("f32") {
        handle::<f32>(&path, config, &opt)
      } else if path_str.contains("bool") {
        handle::<bool>(&path, config, &opt)
      } else if path_str.contains("micros") {
        handle::<TimestampMicros>(&path, config, &opt)
      } else {
        panic!(
          "Could not determine dtype for file {}!",
          path_str
        );
      };
      stats.extend(stat);
    }
  }

  print_stats(&stats);
}
