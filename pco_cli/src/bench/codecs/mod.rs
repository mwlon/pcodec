use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::fs;
use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use arrow::array::{Array, ArrayRef};

use q_compress::data_types::TimestampMicros;

use crate::bench::codecs::blosc::BloscConfig;
use crate::bench::codecs::parquet::ParquetConfig;
use crate::bench::codecs::pco::PcoConfig;
use crate::bench::codecs::qco::QcoConfig;
use crate::bench::codecs::snappy::SnappyConfig;
use crate::bench::codecs::zstd::ZstdConfig;
use crate::bench::dtypes::Dtype;
use crate::bench::num_vec::NumVec;
use crate::bench::opt::IterOpt;
use crate::bench::{BenchStat, Precomputed};
use crate::dtypes::PcoNumberLike;

mod blosc;
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
  fn get_confs(&self) -> Vec<(&'static str, String)>;
  fn set_conf(&mut self, key: &str, value: String) -> Result<()>;

  fn compress<T: PcoNumberLike>(&self, nums: &[T]) -> Vec<u8>;
  fn decompress<T: PcoNumberLike>(&self, compressed: &[u8]) -> Vec<T>;

  // sad manual dynamic dispatch, but at least we don't need all combinations
  // of (dtype x codec)
  // fn compress_dynamic(&self, num_vec: &NumVec) -> Vec<u8> {
  //   match num_vec {
  //     NumVec::U32(nums) => self.compress(nums),
  //     NumVec::I32(nums) => self.compress(nums),
  //     NumVec::I64(nums) => self.compress(nums),
  //     NumVec::F32(nums) => self.compress(nums),
  //     NumVec::F64(nums) => self.compress(nums),
  //     NumVec::Micros(nums) => self.compress(nums),
  //   }
  // }

  // fn decompress_dynamic(&self, dtype: &str, compressed: &[u8]) -> NumVec {
  //   match dtype {
  //     "u32" => NumVec::U32(self.decompress::<u32>(compressed)),
  //     "i32" => NumVec::I32(self.decompress::<i32>(compressed)),
  //     "i64" => NumVec::I64(self.decompress::<i64>(compressed)),
  //     "f32" => NumVec::F32(self.decompress::<f32>(compressed)),
  //     "f64" => NumVec::F64(self.decompress::<f64>(compressed)),
  //     "micros" => NumVec::Micros(self.decompress::<TimestampMicros>(compressed)),
  //     _ => panic!("unknown dtype {}", dtype),
  //   }
  // }

  fn compare_nums<T: PcoNumberLike>(&self, recovered: &[T], original: &[T]) {
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
      (NumVec::I32(x), NumVec::I32(y)) => self.compare_nums(x, y),
      (NumVec::I64(x), NumVec::I64(y)) => self.compare_nums(x, y),
      (NumVec::F32(x), NumVec::F32(y)) => self.compare_nums(x, y),
      (NumVec::F64(x), NumVec::F64(y)) => self.compare_nums(x, y),
      (NumVec::Micros(x), NumVec::Micros(y)) => self.compare_nums(x, y),
      _ => unreachable!(),
    }
  }
}

pub trait CodecSurface: Debug + Send + Sync {
  fn name(&self) -> &'static str;
  fn set_conf(&mut self, key: &str, value: String) -> Result<()>;
  fn details(&self) -> String;

  fn warmup_iter<T: PcoNumberLike>(
    &self,
    nums: &[T],
    dataset: &str,
    opt: &IterOpt,
  ) -> Result<Precomputed>;
  fn stats_iter<T: PcoNumberLike>(
    &self,
    nums: &[T],
    precomputed: &Precomputed,
    opt: &IterOpt,
  ) -> Result<BenchStat>;

  fn clone_to_box(&self) -> Box<dyn CodecSurface>;
}

impl<C: CodecInternal> CodecSurface for C {
  fn name(&self) -> &'static str {
    self.name()
  }

  fn set_conf(&mut self, key: &str, value: String) -> Result<()> {
    self.set_conf(key, value)
  }

  fn details(&self) -> String {
    let default_confs: HashMap<String, String> = Self::default().get_confs().into();
    let mut res = String::new();
    for (k, v) in self.get_confs() {
      if v != default_confs.get(&k).unwrap() {
        res.push_str(&format!(":{}={}", k, v,));
      }
    }
    res
  }

  fn warmup_iter<T: PcoNumberLike>(
    &self,
    nums: &[T],
    dataset: &str,
    opt: &IterOpt,
  ) -> Result<Precomputed> {
    let dtype = nums.data_type();

    // compress
    let compressed = self.compress(nums);

    // write to disk
    if let Some(dir) = opt.save_dir.as_ref() {
      let save_path = dir.join(format!(
        "{}{}.{}",
        &dataset,
        self.details(),
        self.name(),
      ));
      fs::write(save_path, &compressed)?;
    }

    // decompress
    if !opt.no_decompress {
      let rec_nums = self.decompress(&compressed);

      if !opt.no_assertions {
        self.compare_nums_dynamic(&rec_nums, nums);
      }
    }

    Ok(Precomputed {
      compressed,
      dtype: dtype.to_string(),
    })
  }

  fn stats_iter<T: PcoNumberLike>(
    &self,
    nums: &[T],
    precomputed: &Precomputed,
    opt: &IterOpt,
  ) -> BenchStat {
    // compress
    let compress_dt = if !opt.no_compress {
      let t = Instant::now();
      let _ = self.compress(nums);
      let dt = Instant::now() - t;
      println!("\tcompressed in {:?}", dt);
      dt
    } else {
      Duration::ZERO
    };

    // decompress
    let decompress_dt = if !opt.no_decompress {
      let t = Instant::now();
      let _ = self.decompress(&precomputed.compressed);
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
pub struct CodecConfig(Box<dyn CodecSurface>);

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
      "blosc" => Box::<BloscConfig>::default(),
      _ => return Err(anyhow!("unknown codec: {}", name)),
    };

    for (k, v) in &confs {
      codec.set_conf(k, v.to_string())?;
    }
    let mut confs = confs.into_iter().map(|(k, _v)| k).collect::<Vec<_>>();
    confs.sort_unstable();

    Ok(Self(codec))
  }
}

impl Display for CodecConfig {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}{}", self.0.name(), self.0.details(),)
  }
}

impl Clone for CodecConfig {
  fn clone(&self) -> Self {
    Self(self.0.clone_to_box())
  }
}

impl AsRef<Box<dyn CodecSurface>> for CodecConfig {
  fn as_ref(&self) -> &Box<dyn CodecSurface> {
    &self.0
  }
}
