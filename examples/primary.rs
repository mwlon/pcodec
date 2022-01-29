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

  fn compress(nums: Vec<T>, max_depth: u32) -> Vec<u8> {
    Compressor::<T>::from_config(CompressorConfig {
      max_depth,
      ..Default::default()
    }).simple_compress(&nums)
      .expect("could not compress")
  }

  fn decompress(bytes: Vec<u8>) -> Vec<T> {
    Decompressor::<T>::default()
      .simple_decompress(bytes)
      .expect("could not decompress")
  }

  fn handle(path: &Path, max_depth: u32, output_dir: &str) {
    // compress
    let bytes = fs::read(path).expect("could not read");
    let nums = Self::parse_nums(&bytes);
    let compress_start = Instant::now();
    let compressed = Self::compress(nums.clone(), max_depth);
    println!("COMPRESSED TO {} BYTES IN {:?}", compressed.len(), Instant::now() - compress_start);

    let fname = basename_no_ext(&path);
    let output_path = format!("{}/{}.qco", &output_dir, fname);
    fs::write(
      &output_path,
      &compressed,
    ).expect("couldn't write");

    // decompress
    let decompress_start = Instant::now();
    let rec_nums = Self::decompress(compressed);
    println!("DECOMPRESSED IN {:?}", Instant::now() - decompress_start);

    // make sure everything came back correct
    for i in 0..rec_nums.len() {
      if !rec_nums[i].num_eq(&nums[i]) {
        println!(
          "{} num {} -> {}",
          i,
          nums[i],
          rec_nums[i]
        );
        panic!("Failed to recover nums by compressing and decompressing!");
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

fn main() {
  let args: Vec<String> = env::args().collect();
  let max_depth: u32 = if args.len() >= 2 {
    args[1].parse().expect("invalid max depth")
  } else {
    6
  };
  let substring_filter = if args.len() >= 3 {
    args[2].clone()
  } else {
    "".to_string()
  };

  let files = fs::read_dir("examples/data/binary").expect("couldn't read");
  let output_dir = format!("examples/data/q_compressed_{}", max_depth);
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

    println!("\nfile: {}", path.display());
    if path_str.contains("i64") {
      I64Handler::handle(&path, max_depth, &output_dir);
    } else if path_str.contains("f64") {
      F64Handler::handle(&path, max_depth, &output_dir);
    } else if path_str.contains("bool8") {
      BoolHandler::handle(&path, max_depth, &output_dir);
    } else {
      panic!("Could not determine dtype for file {}!", path_str);
    };
  }
}
