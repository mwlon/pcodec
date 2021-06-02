use std::env;
use std::fs;
use std::io::ErrorKind;
use std::time::SystemTime;
use std::path::PathBuf;

use quantile_compression::compressor::QuantileCompressor;
use quantile_compression::bit_reader::BitReader;
use quantile_compression::bits::{bits_to_bytes, bits_to_string};

fn basename_no_ext(path: &PathBuf) -> String {
  let basename = path
    .file_name()
    .expect("weird path")
    .to_str()
    .expect("not unicode");
  match basename.find(".") {
    Some(i) => basename[..i].to_string(),
    _ => basename.to_string(),
  }
}

fn main() {
  let args: Vec<String> = env::args().collect();
  let max_depth: u32 = if args.len() > 1 {
    args[1].parse().expect("invalid max depth")
  } else {
    5
  };

  let files = fs::read_dir("data/txt").expect("couldn't read");
  let output_dir = format!("data/quantile_compressed_{}", max_depth);
  match fs::create_dir(&output_dir) {
    Ok(()) => (),
    Err(e) => match e.kind() {
      ErrorKind::AlreadyExists => (),
      _ => panic!(e)
    }
  }

  for f in files {
    // COMPRESS
    let path = f.unwrap().path();
    println!("\nfile: {}", path.display());
    let ints = &fs::read_to_string(&path)
      .expect("couldn't read")
      .split("\n")
      .map(|s| s.parse::<i64>().unwrap())
      .collect();
    let compressor = QuantileCompressor::train(ints, max_depth).expect("could not train");
    println!("compressor:\n{}", compressor);
    let fname = basename_no_ext(&path);

    let output_path = format!("{}/{}.qco", &output_dir, fname);
    fs::write(
      &output_path,
      bits_to_bytes(compressor.compress_series(ints)),
    ).expect("couldn't write");

    // DECOMPRESS
    let start_t = SystemTime::now();
    let bytes = fs::read(output_path).expect("couldn't read");
    let mut bit_reader = BitReader::new(bytes);
    let bit_reader_ptr = &mut bit_reader;
    let decompressor = QuantileCompressor::from_bytes(bit_reader_ptr);
    println!("decompressor:\n{}", decompressor);
    let rec_ints = decompressor.decompress(bit_reader_ptr);
    println!("{} ints: {} {}", rec_ints.len(), rec_ints.first().unwrap(), rec_ints.last().unwrap());
    let end_t = SystemTime::now();
    let dt = end_t.duration_since(start_t).expect("can't take dt");
    println!("DECOMPRESSED IN {:?}", dt);
    for i in 0..rec_ints.len() {
      if rec_ints[i] != ints[i] {
        println!(
          "{} int {} -> {} -> {}",
          i,
          ints[i],
          bits_to_string(&compressor.compress_int(ints[i])),
          rec_ints[i]
        );
        panic!("failed to recover ints by compressing and decompressing!");
      }
    }
  }
}