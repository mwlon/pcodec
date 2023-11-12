use std::fmt::{Debug, Display, Formatter};
use std::fs;
use std::io::ErrorKind;
use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};

use q_compress::data_types::TimestampMicros;

use crate::codecs::parquet::ParquetConfig;
use crate::codecs::pco::PcoConfig;
use crate::codecs::qco::QcoConfig;
use crate::codecs::snappy::SnappyConfig;
use crate::codecs::zstd::ZstdConfig;
use crate::dtypes::Dtype;
use crate::num_vec::NumVec;
use crate::opt::HandlerOpt;
use crate::{BenchStat, Precomputed, BASE_DIR};

mod parquet;
mod pco;
mod qco;
mod snappy;
pub mod utils;
mod zstd;

// Unfortunately we can't make a Box<dyn this> because it has generic
// functions, so we use a wrapping trait (CodecSurface) to manually dynamic
// dispatch.
trait CodecInternal: Clone + Debug + Send + Sync + Default + 'static {
  fn name(&self) -> &'static str;
  // panics if not found because that's a bug
  fn get_conf(&self, key: &str) -> String;
  fn set_conf(&mut self, key: &str, value: String) -> Result<()>;

  fn compress<T: Dtype>(&self, nums: &[T]) -> Vec<u8>;
  fn decompress<T: Dtype>(&self, compressed: &[u8]) -> Vec<T>;

  // sad manual dynamic dispatch, but at least we don't need all combinations
  // of (dtype x codec)
  fn compress_dynamic(&self, num_vec: &NumVec) -> Vec<u8> {
    match num_vec {
      NumVec::U32(nums) => self.compress(nums),
      NumVec::I64(nums) => self.compress(nums),
      NumVec::F64(nums) => self.compress(nums),
      NumVec::Micros(nums) => self.compress(nums),
    }
  }

  fn decompress_dynamic(&self, dtype: &str, compressed: &[u8]) -> NumVec {
    match dtype {
      "u32" => NumVec::U32(self.decompress::<u32>(compressed)),
      "i64" => NumVec::I64(self.decompress::<i64>(compressed)),
      "f64" => NumVec::F64(self.decompress::<f64>(compressed)),
      "micros" => NumVec::Micros(self.decompress::<TimestampMicros>(compressed)),
      _ => panic!("unknown dtype {}", dtype),
    }
  }

  fn compare_nums<T: Dtype>(&self, recovered: &[T], original: &[T]) {
    assert_eq!(recovered.len(), original.len());
    for (i, (x, y)) in recovered.iter().zip(original.iter()).enumerate() {
      assert_eq!(
        x.to_unsigned(),
        y.to_unsigned(),
        "{} != {} at {}",
        x,
        y,
        i
      );
    }
  }

  fn compare_nums_dynamic(&self, recovered: &NumVec, original: &NumVec) {
    match (recovered, original) {
      (NumVec::U32(x), NumVec::U32(y)) => self.compare_nums(x, y),
      (NumVec::I64(x), NumVec::I64(y)) => self.compare_nums(x, y),
      (NumVec::F64(x), NumVec::F64(y)) => self.compare_nums(x, y),
      (NumVec::Micros(x), NumVec::Micros(y)) => self.compare_nums(x, y),
      _ => panic!("should be unreachable"),
    }
  }
}

pub trait CodecSurface: Debug + Send + Sync {
  fn name(&self) -> &'static str;
  // panics if not found because that's a bug
  fn get_conf(&self, key: &str) -> String;
  fn set_conf(&mut self, key: &str, value: String) -> Result<()>;
  fn details(&self, confs: &[String]) -> String;

  fn warmup_iter(
    &self,
    num_vec: &NumVec,
    fname: &str,
    opt: &HandlerOpt,
  ) -> Precomputed;
  fn stats_iter(&self, nums: &NumVec, precomputed: &Precomputed, opt: &HandlerOpt) -> BenchStat;

  fn clone_to_box(&self) -> Box<dyn CodecSurface>;
}

impl<C: CodecInternal> CodecSurface for C {
  fn name(&self) -> &'static str {
    self.name()
  }

  fn get_conf(&self, key: &str) -> String {
    self.get_conf(key)
  }

  fn set_conf(&mut self, key: &str, value: String) -> Result<()> {
    self.set_conf(key, value)
  }

  fn details(&self, confs: &[String]) -> String {
    let default = Self::default();
    let mut res = String::new();
    for k in confs {
      let v = self.get_conf(k);
      if v != default.get_conf(k) {
        res.push_str(&format!(":{}={}", k, v,));
      }
    }
    res
  }

  fn warmup_iter(
    &self,
    nums: &NumVec,
    fname: &str,
    opt: &HandlerOpt,
  ) -> Precomputed {
    let dtype = nums.dtype_str();

    // compress
    let compressed = self.compress_dynamic(nums);
    println!(
      "\nwarmup: compressed to {} bytes",
      compressed.len(),
    );

    // write to disk
    let output_dir = format!("{}/{}", BASE_DIR, self.name());
    let output_path = format!("{}/{}", output_dir, fname);

    match fs::create_dir(&output_dir) {
      Ok(()) => (),
      Err(e) => match e.kind() {
        ErrorKind::AlreadyExists => (),
        _ => panic!("{}", e),
      },
    }
    fs::write(output_path, &compressed).expect("couldn't write");

    // decompress
    let rec_nums = self.decompress_dynamic(dtype, &compressed);

    if !opt.no_assertions {
      self.compare_nums_dynamic(&rec_nums, nums);
    }

    Precomputed {
      compressed,
      dtype: dtype.to_string(),
    }
  }

  fn stats_iter(&self, nums: &NumVec, precomputed: &Precomputed, opt: &HandlerOpt) -> BenchStat {
    // compress
    let compress_dt = if !opt.no_compress {
      let t = Instant::now();
      let _ = self.compress_dynamic(nums);
      let dt = Instant::now() - t;
      println!("\tcompressed in {:?}", dt);
      dt
    } else {
      Duration::ZERO
    };

    // decompress
    let decompress_dt = if !opt.no_decompress {
      let t = Instant::now();
      let _ = self.decompress_dynamic(&precomputed.dtype, &precomputed.compressed);
      let dt = Instant::now() - t;
      println!("\tdecompressed in {:?}", dt);
      dt
    } else {
      Duration::ZERO
    };

    BenchStat {
      compressed_size: precomputed.compressed.len(),
      compress_dt,
      decompress_dt,
    }
  }

  fn clone_to_box(&self) -> Box<dyn CodecSurface> {
    Box::new(self.clone())
  }
}

#[derive(Debug)]
pub struct CodecConfig {
  pub inner: Box<dyn CodecSurface>,
  pub confs: Vec<String>,
}

impl FromStr for CodecConfig {
  type Err = anyhow::Error;

  fn from_str(s: &str) -> Result<Self> {
    let parts = s.split(':').collect::<Vec<_>>();
    let name = parts[0];
    let mut confs = Vec::new();
    for &part in &parts[1..] {
      let kv_vec = part.split('=').collect::<Vec<_>>();
      if kv_vec.len() != 2 {
        return Err(anyhow!(
          "codec config {} is not a key=value pair",
          part
        ));
      }
      confs.push((kv_vec[0].to_string(), kv_vec[1].to_string()));
    }

    let mut codec: Box<dyn CodecSurface> = match name {
      "p" | "pco" | "pcodec" => Box::<PcoConfig>::default(),
      "q" | "qco" | "q_compress" => Box::<QcoConfig>::default(),
      "zstd" => Box::<ZstdConfig>::default(),
      "snap" | "snappy" => Box::<SnappyConfig>::default(),
      "parq" | "parquet" => Box::<ParquetConfig>::default(),
      _ => return Err(anyhow!("unknown codec: {}", name)),
    };

    for (k, v) in &confs {
      codec.set_conf(k, v.to_string())?;
    }
    let mut confs = confs.into_iter().map(|(k, _v)| k).collect::<Vec<_>>();
    confs.sort_unstable();

    Ok(Self {
      inner: codec,
      confs,
    })
  }
}

impl Display for CodecConfig {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "{}{}",
      self.inner.name(),
      self.inner.details(&self.confs)
    )
  }
}

impl Clone for CodecConfig {
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone_to_box(),
      confs: self.confs.clone(),
    }
  }
}

impl CodecConfig {
  pub fn details(&self) -> String {
    self.inner.details(&self.confs)
  }
}
