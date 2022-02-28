use std::env;
use std::fs;
use std::io::ErrorKind;
use std::time::Instant;

const BASE_DIR: &str = "q_compress/examples/data";

fn main() {
  let args: Vec<String> = env::args().collect();
  let compression_level: i32 = if args.len() >= 2 {
    args[1].parse().expect("invalid compression level")
  } else {
    3
  };
  let substring_filter = if args.len() >= 3 {
    args[2].clone()
  } else {
    "".to_string()
  };

  let subfolder = format!("zstd_{}", compression_level);
  let output_dir = format!("{}/{}", BASE_DIR, &subfolder);
  match fs::create_dir(&output_dir) {
    Ok(()) => (),
    Err(e) => match e.kind() {
      ErrorKind::AlreadyExists => (),
      _ => panic!("{}", e)
    }
  }
  let files = fs::read_dir(format!("{}/binary", BASE_DIR))
    .expect("couldn't read");

  for file in files {
    let path = file.unwrap().path();
    let path_str = path.to_str().unwrap();
    if !path_str.contains(&substring_filter) {
      continue;
    }

    println!("\nRUNNING ON {:?}", path.file_name().unwrap());
    let out_path_str = path_str
      .replace("binary", &subfolder)
      .replace(".bin", ".zstd");
    let binary = fs::read(path_str).unwrap();

    // We may want to include the step of number -> bytes in the future to
    // be more fair. Right now we're giving zstd a slight advantage.
    let compress_start = Instant::now();
    let compressed = zstd::encode_all(binary.as_slice(), compression_level).unwrap();
    let compress_end = Instant::now();
    println!("COMPRESSED TO {} BYTES IN {:?}", compressed.len(), compress_end - compress_start);

    fs::write(out_path_str, &compressed).unwrap();

    let decompress_start = Instant::now();
    let decompressed = zstd::decode_all(compressed.as_slice()).unwrap();
    let decompress_end = Instant::now();
    println!("DECOMPRESSED to {} BYTES IN {:?}", decompressed.len(), decompress_end - decompress_start);
  }
}