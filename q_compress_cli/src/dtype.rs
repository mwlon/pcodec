use std::convert::TryFrom;
use std::str::FromStr;

use anyhow::anyhow;
use anyhow::Result;
use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};

use q_compress::data_types::{NumberLike, TimestampMicros, TimestampNanos};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DType {
  Bool,
  F32,
  F64,
  I32,
  I64,
  I128,
  U32,
  U64,
  Micros,
  Nanos,
}

impl FromStr for DType {
  type Err = anyhow::Error;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    let res = match s.to_lowercase().as_str() {
      "bool" => DType::Bool,
      "f32" => DType::F32,
      "f64" => DType::F64,
      "i32" => DType::I32,
      "i64" => DType::I64,
      "i128" => DType::I128,
      "u32" => DType::U32,
      "u64" => DType::U64,
      "micros" | "timestampmicros" => DType::Micros,
      "nanos" | "timestampnanos" => DType::Nanos,
      _ => {
        return Err(anyhow!("unknown dtype {}", s));
      }
    };
    Ok(res)
  }
}

impl TryFrom<u8> for DType {
  type Error = anyhow::Error;
  fn try_from(header_byte: u8) -> Result<Self, Self::Error> {
    let res = match header_byte {
      bool::HEADER_BYTE => DType::Bool,
      f32::HEADER_BYTE => DType::F32,
      f64::HEADER_BYTE => DType::F64,
      i32::HEADER_BYTE => DType::I32,
      i64::HEADER_BYTE => DType::I64,
      i128::HEADER_BYTE => DType::I128,
      TimestampMicros::HEADER_BYTE => DType::Micros,
      TimestampNanos::HEADER_BYTE => DType::Nanos,
      u32::HEADER_BYTE => DType::U32,
      u64::HEADER_BYTE => DType::U64,
      _ => {
        return Err(anyhow!("unknown data type byte {}", header_byte));
      }
    };
    Ok(res)
  }
}

impl DType {
  pub fn to_arrow(self) -> Result<ArrowDataType> {
    let res = match self {
      DType::Bool => ArrowDataType::Boolean,
      DType::F32 => ArrowDataType::Float32,
      DType::F64 => ArrowDataType::Float64,
      DType::I32 => ArrowDataType::Int32,
      DType::I64 => ArrowDataType::Int64,
      DType::U32 => ArrowDataType::UInt32,
      DType::U64 => ArrowDataType::UInt64,
      DType::Micros => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
      DType::Nanos => ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
      _ => {
        return Err(anyhow!("unable to convert q_compress dtype {:?} to arrow", self));
      }
    };
    Ok(res)
  }

  pub fn from_arrow(arrow_dtype: &ArrowDataType) -> Result<Self> {
    let res = match arrow_dtype {
      ArrowDataType::Boolean => DType::Bool,
      ArrowDataType::Float32 => DType::F32,
      ArrowDataType::Float64 => DType::F64,
      ArrowDataType::Int32 => DType::I32,
      ArrowDataType::Int64 => DType::I64,
      ArrowDataType::UInt32 => DType::U32,
      ArrowDataType::UInt64 => DType::U64,
      ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => DType::Micros,
      ArrowDataType::Timestamp(TimeUnit::Nanosecond, _) => DType::Nanos,
      _ => {
        return Err(anyhow!("unable to convert arrow dtype {:?} to q_compress", arrow_dtype))
      }
    };
    Ok(res)
  }
}
