use crate::bit_reader::{BitReader, BitReaderBuilder};
use crate::bit_writer::BitWriter;
use crate::data_types::{NumberLike, UnsignedLike};
use crate::errors::PcoResult;
use crate::{standalone, ChunkConfig};
use std::fs;
use std::fs::File;
use std::path::PathBuf;
use std::str::FromStr;

fn get_asset_dir() -> PathBuf {
  PathBuf::from_str(env!("CARGO_MANIFEST_DIR"))
    .unwrap()
    .join("assets")
}

fn get_pco_path(version: &str, name: &str) -> PathBuf {
  get_asset_dir().join(format!(
    "v{}_{}.pco",
    version.replace(".", "_"),
    name,
  ))
}

fn assert_nums_eq<T: NumberLike>(x: &[T], y: &[T]) {
  assert_eq!(x.len(), y.len());
  for (i, (x, y)) in x.iter().zip(y).enumerate() {
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

fn assert_compatible<T: NumberLike>(version: &str, name: &str, expected: &[T]) -> PcoResult<()> {
  let pco_path = get_pco_path(version, name);

  let compressed = fs::read(pco_path)?;
  let decompressed = standalone::auto_decompress::<T>(&compressed)?;

  assert_nums_eq(&decompressed, &expected);
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

  println!("HERE");
  let pco_path = get_pco_path(version, name);
  println!("{:?}", pco_path);
  if pco_path.exists() {
    return Ok(());
  }
  println!("THERE {:?}", pco_path);

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
    let name = "classic";
    let version = "0.0.0";
    let nums = (0_i32..2000).collect::<Vec<_>>();
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
    nums[1337] += 0.001;
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
    let version = "0.1.0";
    let name = "delta_int_mult";
    let mut nums = (0..2000).map(|i| i * 1000).collect::<Vec<_>>();
    nums[1337] -= 1;
    let config = ChunkConfig {
      delta_encoding_order: Some(1),
      ..Default::default()
    };
    simple_write_if_version_matches(version, name, &nums, &config)?;
    println!("!");
    assert_compatible(version, name, &nums)?;
    Ok(())
  }
}
