use std::fs;
use std::path::PathBuf;
use std::str::FromStr;

use crate::data_types::NumberLike;
use crate::errors::PcoResult;
use crate::{standalone, ChunkConfig};

fn get_asset_dir() -> PathBuf {
  PathBuf::from_str(env!("CARGO_MANIFEST_DIR"))
    .unwrap()
    .join("assets")
}

fn get_pco_path(version: &str, name: &str) -> PathBuf {
  get_asset_dir().join(format!(
    "v{}_{}.pco",
    version.replace('.', "_"),
    name,
  ))
}

fn assert_nums_eq<T: NumberLike>(x: &[T], y: &[T]) {
  assert_eq!(x.len(), y.len());
  for (i, (x, y)) in x.iter().zip(y).enumerate() {
    assert_eq!(
      x.to_latent_ordered(),
      y.to_latent_ordered(),
      "{} != {} at {}",
      x,
      y,
      i
    );
  }
}

fn assert_compatible<T: NumberLike>(version: &str, name: &str, expected: &[T]) -> PcoResult<()> {
  let pco_path = get_pco_path(version, name);

  let compressed = fs::read(pco_path)?;
  let decompressed = standalone::simple_decompress::<T>(&compressed)?;

  assert_nums_eq(&decompressed, expected);
  Ok(())
}

fn simple_write_if_version_matches<T: NumberLike>(
  version: &str,
  name: &str,
  nums: &[T],
  config: &ChunkConfig,
) -> PcoResult<()> {
  if version != env!("CARGO_PKG_VERSION") {
    return Ok(());
  }

  let pco_path = get_pco_path(version, name);
  if pco_path.exists() {
    return Ok(());
  }

  fs::write(
    pco_path,
    standalone::simple_compress(nums, config)?,
  )?;
  Ok(())
}

#[cfg(test)]
mod tests {
  use crate::errors::PcoResult;
  use crate::tests::compatibility::{assert_compatible, simple_write_if_version_matches};
  use crate::ChunkConfig;

  #[test]
  fn v0_0_0_classic() -> PcoResult<()> {
    let version = "0.0.0";
    let name = "classic";
    let nums = (0_i32..1000).chain(2000..3000).collect::<Vec<_>>();
    let config = ChunkConfig {
      delta_encoding_order: Some(0),
      ..Default::default()
    };
    simple_write_if_version_matches(version, name, &nums, &config)?;
    assert_compatible(version, name, &nums)?;
    Ok(())
  }

  #[test]
  fn v0_0_0_delta_float_mult() -> PcoResult<()> {
    let version = "0.0.0";
    let name = "delta_float_mult";
    let mut nums = (0..2000).map(|i| i as f32).collect::<Vec<_>>();
    nums[1337] += 1.001;
    let config = ChunkConfig {
      delta_encoding_order: Some(1),
      ..Default::default()
    };
    simple_write_if_version_matches(version, name, &nums, &config)?;
    assert_compatible(version, name, &nums)?;
    Ok(())
  }

  #[test]
  fn v0_1_0_delta_int_mult() -> PcoResult<()> {
    // starting at 0.1.0 because 0.0.0 had GCD mode (no longer supported)
    // instead of int mult
    let version = "0.1.0";
    let name = "delta_int_mult";
    let mut nums = (0..2000).map(|i| i * 1000).collect::<Vec<_>>();
    nums[1337] -= 1;
    let config = ChunkConfig {
      delta_encoding_order: Some(1),
      ..Default::default()
    };
    simple_write_if_version_matches(version, name, &nums, &config)?;
    assert_compatible(version, name, &nums)?;
    Ok(())
  }

  #[test]
  fn v0_1_1_classic() -> PcoResult<()> {
    // v0.1.1 introduced standalone versioning, separate from wrapped versioning
    let version = "0.1.1";
    let name = "standalone_versioned";
    let nums = vec![];
    let config = ChunkConfig::default();
    simple_write_if_version_matches::<f32>(version, name, &nums, &config)?;
    assert_compatible(version, name, &nums)?;
    Ok(())
  }
}
