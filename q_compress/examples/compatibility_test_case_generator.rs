use q_compress::data_types::{NumberLike, TimestampMicros};
use q_compress::{Compressor, CompressorConfig};
use rand::Rng;
use std::time::{SystemTime, Duration};
use std::ops::AddAssign;

const BASE_DIR: &str = "q_compress/assets";

fn write_case<T: NumberLike>(version: &str, case_version: &str, name: &str, nums: Vec<T>, config: CompressorConfig) {
  if version != case_version {
    return;
  }

  let compressor = Compressor::<T>::from_config(config);
  let compressed = compressor.simple_compress(&nums);
  let raw = nums.iter()
    // .flat_map(|&x| T::bytes_from(x)) // for 0.4 to 0.5
    .flat_map(|&x| x.to_bytes()) // for 0.6+
    .collect::<Vec<u8>>();
  std::fs::write(
    format!("{}/v{}_{}.qco", BASE_DIR, version, name),
    compressed,
  ).expect("write qco");
  std::fs::write(
    format!("{}/v{}_{}.bin", BASE_DIR, version, name),
    raw,
  ).expect("write bin");
}

fn main() {
  let major_version = env!("CARGO_PKG_VERSION_MAJOR");
  let minor_version = env!("CARGO_PKG_VERSION_MINOR");
  let version = format!("{}.{}", major_version, minor_version);
  let mut rng = rand::thread_rng();

  write_case(
    &version,
    "0.4",
    "i64_empty",
    Vec::<i64>::new(),
    CompressorConfig::default(),
  );

  let mut i32_2k = Vec::new();
  for _ in 0..2000 {
    let num = if rng.gen_bool(0.25) {
      rng.gen_range(0_i32..100)
    } else {
      rng.gen_range(1000_i32..1100)
    };
    i32_2k.push(num);
  }
  write_case(
    &version,
    "0.4",
    "i32_2k",
    i32_2k,
    CompressorConfig::default(),
  );

  let mut f32_2k = Vec::new();
  for _ in 0..2000 {
    f32_2k.push(10.0 / (1.0 + 9.0 * rng.gen::<f32>()));
  }
  write_case(
    &version,
    "0.4",
    "f32_2k",
    f32_2k,
    CompressorConfig::default(),
  );

  let mut bool_sparse_2k = Vec::new();
  for _ in 0..2000 {
    bool_sparse_2k.push(rng.gen_bool(0.1));
  }
  write_case(
    &version,
    "0.4",
    "bool_sparse_2k",
    bool_sparse_2k,
    CompressorConfig::default(),
  );

  let mut timestamp_deltas_2k = Vec::new();
  let mut t = SystemTime::now();
  for _ in 0..2000 {
    t.add_assign(Duration::from_secs_f64(0.5 + rng.gen::<f64>()));
    timestamp_deltas_2k.push(TimestampMicros::from(t));
  }
  write_case(
    &version,
    "0.6",
    "timestamp_deltas_2k",
    timestamp_deltas_2k,
    CompressorConfig {
      delta_encoding_order: 1,
      ..Default::default()
    },
  );

  let mut dispersed_shorts = Vec::new();
  for _ in 0..64 {
    for i in 0_u16..20 {
      dispersed_shorts.push(i * 4);
    }
  }
  // really using 0.9.1
  write_case(
    &version,
    "0.9",
    "dispersed_shorts",
    dispersed_shorts,
    CompressorConfig::default(),
  )
}