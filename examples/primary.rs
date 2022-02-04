use std::convert::TryInto;
use std::env;
use std::fs;
use std::io::ErrorKind;
use std::path::Path;
use std::time::Instant;

use q_compress::{Compressor, CompressorConfig, Decompressor};
use q_compress::types::NumberLike;

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

trait DtypeHandler<T: 'static> where T: NumberLike {
  fn parse_nums(bytes: &[u8]) -> Vec<T>;

  fn compress(nums: Vec<T>, config: CompressorConfig) -> Vec<u8> {
    Compressor::<T>::from_config(config)
      .simple_compress(&nums)
      .expect("could not compress")
  }

  fn decompress(bytes: Vec<u8>) -> Vec<T> {
    Decompressor::<T>::default()
      .simple_decompress(bytes)
      .expect("could not decompress")
  }

  fn handle(
    path: &Path,
    output_dir: &str,
    config: CompressorConfig,
  ) {
    // compress
    let bytes = fs::read(path).expect("could not read");
    let nums = Self::parse_nums(&bytes);
    let mut fname = basename_no_ext(&path);
    if config.delta_encoding_order > 0 {
      fname.push_str(&format!("_del={}", config.delta_encoding_order));
    }
    let output_path = format!("{}/{}.qco", &output_dir, fname);

    let compress_start = Instant::now();
    let compressed = Self::compress(nums.clone(), config);
    println!("COMPRESSED TO {} BYTES IN {:?}", compressed.len(), Instant::now() - compress_start);

    fs::write(
      &output_path,
      &compressed,
    ).expect("couldn't write");

    // decompress
    let decompress_start = Instant::now();
    let rec_nums = Self::decompress(compressed);
    println!("DECOMPRESSED IN {:?}", Instant::now() - decompress_start);

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
  }
}

struct I64Handler {}

impl DtypeHandler<i64> for I64Handler {
  fn parse_nums(bytes: &[u8]) -> Vec<i64> {
    bytes
      .chunks(8)
      // apparently numpy writes in le order
      .map(|chunk| i64::from_le_bytes(chunk.try_into().expect("incorrect # of bytes in file")))
      .collect()
  }
}

struct F64Handler {}

impl DtypeHandler<f64> for F64Handler {
  fn parse_nums(bytes: &[u8]) -> Vec<f64> {
    bytes
      .chunks(8)
      .map(|chunk| f64::from_le_bytes(chunk.try_into().expect("incorrect # of bytes in file")))
      .collect()
  }
}

struct BoolHandler {}

impl DtypeHandler<bool> for BoolHandler {
  fn parse_nums(bytes: &[u8]) -> Vec<bool> {
    bytes
      .chunks(1)
      .map(|chunk| u8::from_le_bytes(chunk.try_into().expect("incorrect # of bytes in file")) != 0)
      .collect()
  }
}

fn get_configs(path_str: &str, compression_level: u32) -> Vec<CompressorConfig> {
  let mut res = vec![
    CompressorConfig {
      compression_level,
      ..Default::default()
    }
  ];
  if path_str.contains("slow_cosine") {
    for delta_encoding_order in [1, 2, 7] {
      res.push(CompressorConfig {
        compression_level,
        delta_encoding_order,
      });
    }
  } else if path_str.contains("extremes") || path_str.contains("bool") || path_str.contains("edge") {
    res.push(CompressorConfig {
      compression_level,
      delta_encoding_order: 1,
    });
  }
  res
}

fn main() {
  let args: Vec<String> = env::args().collect();
  let compression_level: u32 = if args.len() >= 2 {
    args[1].parse().expect("invalid compression level")
  } else {
    6
  };
  let substring_filter = if args.len() >= 3 {
    args[2].clone()
  } else {
    "".to_string()
  };

  let files = fs::read_dir("examples/data/binary").expect("couldn't read");
  let output_dir = format!("examples/data/q_compressed_{}", compression_level);
  match fs::create_dir(&output_dir) {
    Ok(()) => (),
    Err(e) => match e.kind() {
      ErrorKind::AlreadyExists => (),
      _ => panic!("{}", e)
    }
  }

  for f in files {
    let path = f.unwrap().path();
    let path_str = path.to_str().unwrap();
    if !path_str.contains(&substring_filter) {
      println!("skipping file that doesn't match substring: {}", path.display());
      continue;
    }

    for config in get_configs(path_str, compression_level) {
      println!("\nfile: {} config: {:?}", path.display(), config);
      if path_str.contains("i64") {
        I64Handler::handle(&path, &output_dir, config);
      } else if path_str.contains("f64") {
        F64Handler::handle(&path, &output_dir, config);
      } else if path_str.contains("bool8") {
        BoolHandler::handle(&path, &output_dir, config);
      } else {
        panic!("Could not determine dtype for file {}!", path_str);
      };
    }
  }
}
