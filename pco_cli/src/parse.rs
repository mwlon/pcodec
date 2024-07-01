use std::str::FromStr;

use anyhow::anyhow;
use arrow::datatypes::{DataType, TimeUnit};

use pco::{FloatMultSpec, FloatQuantSpec, IntMultSpec};

pub fn delta_encoding_order(s: &str) -> anyhow::Result<Option<usize>> {
  match s.to_lowercase().as_str() {
    "auto" => Ok(None),
    other => {
      let delta_order = usize::from_str(other)?;
      Ok(Some(delta_order))
    }
  }
}

pub fn int_mult(s: &str) -> anyhow::Result<IntMultSpec> {
  let lowercase = s.to_lowercase();
  let spec = match lowercase.as_str() {
    "enabled" => IntMultSpec::Enabled,
    "disabled" => IntMultSpec::Disabled,
    other => match other.parse::<u64>() {
      Ok(mult) => IntMultSpec::Provided(mult),
      _ => return Err(anyhow!("cannot parse int mult: {}", other)),
    },
  };
  Ok(spec)
}

pub fn float_mult(s: &str) -> anyhow::Result<FloatMultSpec> {
  let lowercase = s.to_lowercase();
  let spec = match lowercase.as_str() {
    "enabled" => FloatMultSpec::Enabled,
    "disabled" => FloatMultSpec::Disabled,
    other => match other.parse::<f64>() {
      Ok(mult) => FloatMultSpec::Provided(mult),
      _ => return Err(anyhow!("cannot parse float mult: {}", other)),
    },
  };
  Ok(spec)
}

pub fn float_quant(s: &str) -> anyhow::Result<FloatQuantSpec> {
  let lowercase = s.to_lowercase();
  let spec = match lowercase.as_str() {
    "enabled" => FloatQuantSpec::Enabled,
    "disabled" => FloatQuantSpec::Disabled,
    other => match other.parse::<u32>() {
      Ok(k) => FloatQuantSpec::Provided(k),
      _ => {
        return Err(anyhow!(
          "cannot parse float quant parameter: {}",
          other
        ))
      }
    },
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
