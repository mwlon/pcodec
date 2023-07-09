mod pco;

use std::fmt::{Debug, Display, Formatter};
use std::io::ErrorKind;
use std::path::Path;
use std::str::FromStr;
use std::time::{Duration, Instant};
use std::{fs, mem};

use crate::codecs::pco::PcoConfig;
use crate::opt::HandlerOpt;
use crate::{BenchStat, NumberLike, Precomputed, BASE_DIR};
use anyhow::{anyhow, Result};

// Unfortunately we can't make a Box<dyn this> because it has generic
// functions, so we use a wrapping trait (CodecSurface) to manually dynamic
// dispatch.
trait CodecInternal: Clone + Debug + Send + Sync + Default + 'static {
  fn name(&self) -> &'static str;
  // panics if not found because that's a bug
  fn get_conf(&self, key: &str) -> String;
  fn set_conf(&mut self, key: &str, value: String) -> Result<()>;

  fn compress<T: NumberLike>(&self, nums: &[T]) -> Vec<u8>;
  fn decompress<T: NumberLike>(&self, compressed: &[u8]) -> Vec<T>;

  // sad manual dynamic dispatch, but at least we don't need all combinations
  // of (dtype x codec)
  fn compress_dynamic(&self, dtype: &str, raw_bytes: &[u8]) -> Vec<u8> {
    unsafe {
      match dtype {
        "i64" => self.compress::<i64>(mem::transmute(raw_bytes)),
        other => panic!("unknown dtype: {}", other),
      }
    }
  }

  #[allow(clippy::unsound_collection_transmute)]
  fn decompress_dynamic(&self, dtype: &str, compressed: &[u8]) -> Vec<u8> {
    unsafe {
      match dtype {
        "i64" => mem::transmute(self.decompress::<i64>(compressed)),
        other => panic!("unknown dtype: {}", other),
      }
    }
  }
}

pub trait CodecSurface: Debug + Send + Sync {
  fn name(&self) -> &'static str;
  // panics if not found because that's a bug
  fn get_conf(&self, key: &str) -> String;
  fn set_conf(&mut self, key: &str, value: String) -> Result<()>;
  fn details(&self, confs: &[String]) -> String;

  fn warmup_iter(&self, path: &Path, dataset: &str, fname: &str, opt: &HandlerOpt) -> Precomputed;

  fn stats_iter(&self, dataset: &str, precomputed: &Precomputed, opt: &HandlerOpt) -> BenchStat;

  fn clone_to_box(&self) -> Box<dyn CodecSurface>;
}

fn dtype_str(dataset: &str) -> &str {
  dataset.split('_').next().unwrap()
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

  fn warmup_iter(&self, path: &Path, dataset: &str, fname: &str, opt: &HandlerOpt) -> Precomputed {
    // read in data
    let raw_bytes = fs::read(path).expect("could not read");

    // compress
    let dtype = dtype_str(dataset);
    let compressed = self.compress_dynamic(dtype, &raw_bytes);
    println!(
      "\nwarmup for {}: compressed to {} bytes",
      dataset,
      compressed.len(),
    );

    // write to disk
    let output_dir = format!("{}/{}", BASE_DIR, self.name());
    let output_path = format!("{}/{}.{}", output_dir, fname, self.name());

    match fs::create_dir(&output_dir) {
      Ok(()) => (),
      Err(e) => match e.kind() {
        ErrorKind::AlreadyExists => (),
        _ => panic!("{}", e),
      },
    }
    fs::write(output_path, &compressed).expect("couldn't write");

    // decompress
    let rec_raw_bytes = self.decompress_dynamic(dtype, &compressed);

    // TODO make this more informative
    if !opt.no_assertions {
      assert_eq!(rec_raw_bytes, raw_bytes);
    }

    Precomputed {
      raw_bytes,
      compressed,
    }
  }

  fn stats_iter(&self, dataset: &str, precomputed: &Precomputed, opt: &HandlerOpt) -> BenchStat {
    let dtype = dtype_str(dataset);

    // compress
    let compress_dt = if !opt.no_compress {
      let t = Instant::now();
      let _ = self.compress_dynamic(dtype, &precomputed.raw_bytes);
      let dt = Instant::now() - t;
      println!("\tcompressed in {:?}", dt);
      dt
    } else {
      Duration::ZERO
    };

    // decompress
    let decompress_dt = if !opt.no_decompress {
      let t = Instant::now();
      let _ = self.decompress_dynamic(dtype, &precomputed.compressed);
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
      // "q" | "qco" | "q_compress" => Box::new(QcoConfig::default()),
      // "zstd" => Box::new(ZstdConfig::default()),
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
      "{}:{}",
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
