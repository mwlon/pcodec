use q_compress::BitReader;
use q_compress::compressor::Compressor;
use q_compress::decompressor::Decompressor;
use q_compress::types::NumberLike;
use std::convert::TryInto;
use std::env;
use std::fs;
use std::io::ErrorKind;
use std::path::Path;
use std::time::SystemTime;

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

fn bits_to_string(bits: &[bool]) -> String {
  return bits
    .iter()
    .map(|b| if *b {"1"} else {"0"})
    .collect::<Vec<&str>>()
    .join("");
}

trait DtypeHandler<T: 'static> where T: NumberLike {
  fn parse_nums(bytes: &[u8]) -> Vec<T>;
  fn train_compressor(nums: Vec<T>, max_depth: u32) -> Compressor<T> {
    Compressor::<T>::train(nums, max_depth).expect("could not train")
  }
  fn decompressor_from_reader(bit_reader: &mut BitReader) -> Decompressor<T> {
    Decompressor::<T>::from_reader(bit_reader).expect("invalid header")
  }

  fn handle(path: &Path, max_depth: u32, output_dir: &str) {
    // compress
    let bytes = fs::read(path).expect("could not read");
    let nums = Self::parse_nums(&bytes);
    let compress_start = SystemTime::now();
    let compressor = Self::train_compressor(nums.clone(), max_depth);
    println!(
      "compressor in {:?}:\n{:?}",
      SystemTime::now().duration_since(compress_start),
      compressor
    );
    let fname = basename_no_ext(&path);

    let output_path = format!("{}/{}.qco", &output_dir, fname);
    let compressed = compressor.compress(&nums).expect("could not compress");
    let compress_end = SystemTime::now();
    let dt = compress_end.duration_since(compress_start).expect("can't take dt");
    println!("COMPRESSED IN {:?}", dt);

    fs::write(
      &output_path,
      compressed,
    ).expect("couldn't write");

    // decompress
    let bytes = fs::read(output_path).expect("couldn't read");
    let mut bit_reader = BitReader::from(bytes);
    let bit_reader_ptr = &mut bit_reader;
    let decompress_start = SystemTime::now();
    let decompressor = Self::decompressor_from_reader(bit_reader_ptr);
    println!(
      "decompressor in {:?}:\n{:?}",
      SystemTime::now().duration_since(decompress_start),
      decompressor
    );
    let rec_nums = decompressor.decompress(bit_reader_ptr);
    println!("{} nums: {} {}", rec_nums.len(), rec_nums.first().unwrap(), rec_nums.last().unwrap());
    let decompress_end = SystemTime::now();
    let dt = decompress_end.duration_since(decompress_start).expect("can't take dt");
    println!("DECOMPRESSED IN {:?}", dt);

    // make sure everything came back correct
    for i in 0..rec_nums.len() {
      if !rec_nums[i].num_eq(&nums[i]) {
        println!(
          "{} num {} -> {} -> {}",
          i,
          nums[i],
          bits_to_string(&compressor.compress_nums_as_bits(&[nums[i]]).expect("could not compress")),
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

  let files = fs::read_dir("data/binary").expect("couldn't read");
  let output_dir = format!("data/q_compressed_{}", max_depth);
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
