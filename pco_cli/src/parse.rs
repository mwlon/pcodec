use std::str::FromStr;

use anyhow::anyhow;
use arrow::datatypes::{DataType, TimeUnit};

use pco::ModeSpec;

pub fn delta_encoding_order(s: &str) -> anyhow::Result<Option<usize>> {
  match s.to_lowercase().as_str() {
    "auto" => Ok(None),
    other => {
      let delta_order = usize::from_str(other)?;
      Ok(Some(delta_order))
    }
  }
}

pub fn mode_spec(s: &str) -> anyhow::Result<ModeSpec> {
  let lowercase = s.to_lowercase();
  let spec = match lowercase.as_str() {
    "auto" => ModeSpec::Auto,
    "classic" => ModeSpec::Classic,
    other => {
      let mut parts = other.split('@');
      let name = parts.next().unwrap();
      let err = || anyhow!("invalid mode spec: {}", s);
      let value = parts.next().ok_or_else(err)?;
      match name {
        "floatmult" => ModeSpec::TryFloatMult(value.parse()?),
        "floatquant" => ModeSpec::TryFloatQuant(value.parse()?),
        "intmult" => ModeSpec::TryIntMult(value.parse()?),
        _ => return Err(err()),
      }
    }
  };
  Ok(spec)
}

pub fn arrow_dtype(s: &str) -> anyhow::Result<DataType> {
  let name_pairs = [
    ("f16", DataType::Float16),
    ("f32", DataType::Float32),
    ("f64", DataType::Float64),
    ("i16", DataType::Int16),
    ("i32", DataType::Int32),
    ("i64", DataType::Int64),
    ("u16", DataType::UInt16),
    ("u32", DataType::UInt32),
    ("u64", DataType::UInt64),
    (
      "seconds",
      DataType::Timestamp(TimeUnit::Second, None),
    ),
    (
      "millis",
      DataType::Timestamp(TimeUnit::Millisecond, None),
    ),
    (
      "micros",
      DataType::Timestamp(TimeUnit::Microsecond, None),
    ),
    (
      "nanos",
      DataType::Timestamp(TimeUnit::Nanosecond, None),
    ),
  ];

  let lower = s.to_lowercase();
  for (name, dtype) in &name_pairs {
    if name == &lower {
      return Ok(dtype.clone());
    }
  }

  Err(anyhow!(
    "invalid data type: {}. Expected one of: {:?}",
    lower,
    name_pairs
      .iter()
      .map(|(name, _)| name.to_string())
      .collect::<Vec<_>>()
  ))
}
