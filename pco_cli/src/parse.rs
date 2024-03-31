use crate::dtypes;
use anyhow::anyhow;
use arrow::datatypes::{DataType, TimeUnit};
use pco::data_types::CoreDataType;
use pco::{FloatMultSpec, IntMultSpec};

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

pub fn arrow_dtype(s: &str) -> anyhow::Result<DataType> {
  let name_pairs = [
    ("f32", DataType::Float32),
    ("f64", DataType::Float64),
    ("i32", DataType::Int32),
    ("i64", DataType::Int64),
    ("u32", DataType::UInt32),
    ("u64", DataType::UInt64),
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

pub fn core_dtype(s: &str) -> anyhow::Result<CoreDataType> {
  let arrow_dtype = arrow_dtype(s)?;
  dtypes::from_arrow(&arrow_dtype)
}
