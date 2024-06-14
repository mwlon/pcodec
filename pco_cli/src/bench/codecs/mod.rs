use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::fs;
use std::ops::Deref;
use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};

use ::pco::data_types::CoreDataType;
use ::pco::with_core_dtypes;

#[cfg(feature = "full_bench")]
use crate::bench::codecs::blosc::BloscConfig;
use crate::bench::codecs::parquet::ParquetConfig;
use crate::bench::codecs::pco::PcoConfig;
#[cfg(feature = "full_bench")]
use crate::bench::codecs::qco::QcoConfig;
use crate::bench::codecs::snappy::SnappyConfig;
#[cfg(feature = "full_bench")]
use crate::bench::codecs::spdp::SpdpConfig;
use crate::bench::codecs::zstd::ZstdConfig;
use crate::bench::IterOpt;
use crate::bench::{BenchStat, Precomputed};
use crate::dtypes::PcoNumberLike;
use crate::num_vec::NumVec;

#[cfg(feature = "full_bench")]
mod blosc;
mod parquet;
mod pco;
#[cfg(feature = "full_bench")]
mod qco;
mod snappy;
#[cfg(feature = "full_bench")]
mod spdp;
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
  fn compress_dynamic(&self, num_vec: &NumVec) -> Vec<u8> {
    macro_rules! compress {
      {$($name:ident($lname:ident) => $t:ty,)+} => {
        match num_vec {
          $(NumVec::$name(nums) => self.compress(nums),)+
        }
      }
    }
    with_core_dtypes!(compress)
  }

  fn decompress_dynamic(&self, dtype: CoreDataType, compressed: &[u8]) -> NumVec {
    macro_rules! decompress {
      {$($name:ident($lname:ident) => $t:ty,)+} => {
        match dtype {
          $(CoreDataType::$name => NumVec::$name(self.decompress::<$t>(compressed)),)+
        }
      }
    }
    with_core_dtypes!(decompress)
  }
}

pub trait CodecSurface: Debug + Send + Sync {
  fn name(&self) -> &'static str;
  fn set_conf(&mut self, key: &str, value: String) -> Result<()>;
  fn details(&self, explicit: bool) -> String;

  fn warmup_iter(&self, nums_vec: &NumVec, dataset: &str, opt: &IterOpt) -> Result<Precomputed>;
  fn stats_iter(
    &self,
    nums_vec: &NumVec,
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

  fn details(&self, explicit: bool) -> String {
    let asdf = crate::config::ChunkConfigOpt::command_for_update()
    let default_confs: HashMap<&'static str, String> =
      Self::default().get_confs().into_iter().collect();
    let mut res = String::new();
    for (k, v) in self.get_confs() {
      if explicit || &v != default_confs.get(k).unwrap() {
        res.push_str(&format!(":{}={}", k, v,));
      }
    }
    res
  }

  fn warmup_iter(&self, num_vec: &NumVec, dataset: &str, opt: &IterOpt) -> Result<Precomputed> {
    let dtype = num_vec.dtype();

    // compress
    let compressed = self.compress_dynamic(num_vec);

    // write to disk
    if let Some(dir) = opt.save_dir.as_ref() {
      let save_path = dir.join(format!(
        "{}{}.{}",
        &dataset,
        self.details(false),
        self.name(),
      ));
      fs::write(save_path, &compressed)?;
    }

    // decompress
    if !opt.no_decompress {
      let rec_nums = self.decompress_dynamic(dtype, &compressed);

      if !opt.no_assertions {
        rec_nums.check_equal(num_vec);
      }
    }

    Ok(Precomputed { compressed })
  }

  fn stats_iter(
    &self,
    num_vec: &NumVec,
    precomputed: &Precomputed,
    opt: &IterOpt,
  ) -> Result<BenchStat> {
    // compress
    let compress_dt = if !opt.no_compress {
      let t = Instant::now();
      let _ = self.compress_dynamic(num_vec);
      Instant::now() - t
    } else {
      Duration::ZERO
    };

    // decompress
    let decompress_dt = if !opt.no_decompress {
      let t = Instant::now();
      let _ = self.decompress_dynamic(num_vec.dtype(), &precomputed.compressed);
      Instant::now() - t
    } else {
      Duration::ZERO
    };

    Ok(BenchStat {
      compressed_size: precomputed.compressed.len(),
      compress_dt,
      decompress_dt,
    })
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
      #[cfg(feature = "full_bench")]
      "blosc" => Box::<BloscConfig>::default(),
      "parquet" => Box::<ParquetConfig>::default(),
      "pco" | "pcodec" => Box::<PcoConfig>::default(),
      #[cfg(feature = "full_bench")]
      "qco" | "q_compress" => Box::<QcoConfig>::default(),
      "snap" | "snappy" => Box::<SnappyConfig>::default(),
      #[cfg(feature = "full_bench")]
      "spdp" => Box::<SpdpConfig>::default(),
      "zstd" | "zstandard" => Box::<ZstdConfig>::default(),
      _ => {
        return Err(anyhow!(
          "Unknown codec: {}. Perhaps rebuild with the full_bench feature?",
          name
        ))
      }
    };

    for (k, v) in &confs {
      codec.set_conf(k, v.to_string()).with_context(|| {
        format!(
          "explicit conf state: {}{}",
          codec.name(),
          codec.details(true)
        )
      })?;
    }
    let mut confs = confs.into_iter().map(|(k, _v)| k).collect::<Vec<_>>();
    confs.sort_unstable();

    Ok(Self(codec))
  }
}

impl Display for CodecConfig {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "{}{}",
      self.0.name(),
      self.0.details(false),
    )
  }
}

impl Clone for CodecConfig {
  fn clone(&self) -> Self {
    Self(self.0.clone_to_box())
  }
}

impl Deref for CodecConfig {
  type Target = Box<dyn CodecSurface>;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}
