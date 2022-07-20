use std::convert::TryFrom;
use std::str::FromStr;

use anyhow::anyhow;
use anyhow::Result;
use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};

use q_compress::data_types::{NumberLike, TimestampMicros, TimestampNanos, TimestampMicros96, TimestampNanos96};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(test, derive(enum_iterator::IntoEnumIterator))]
pub enum DType {
  Bool,
  F32,
  F64,
  I16,
  I32,
  I64,
  I128,
  TimestampMicros,
  TimestampMicros96,
  TimestampNanos,
  TimestampNanos96,
  U16,
  U32,
  U64,
  U128,
}

impl FromStr for DType {
  type Err = anyhow::Error;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    let res = match s.to_lowercase().as_str() {
      "bool" => DType::Bool,
      "f32" => DType::F32,
      "f64" => DType::F64,
      "i16" => DType::I16,
      "i32" => DType::I32,
      "i64" => DType::I64,
      "i128" => DType::I128,
      "u16" => DType::U16,
      "u32" => DType::U32,
      "u64" => DType::U64,
      "u128" => DType::U128,
      "micros" | "timestampmicros" => DType::TimestampMicros,
      "micros96" | "timestampmicros96" => DType::TimestampMicros96,
      "nanos" | "timestampnanos" => DType::TimestampNanos,
      "nanos96" | "timestampnanos96" => DType::TimestampNanos96,
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
      i16::HEADER_BYTE => DType::I16,
      i32::HEADER_BYTE => DType::I32,
      i64::HEADER_BYTE => DType::I64,
      i128::HEADER_BYTE => DType::I128,
      TimestampMicros::HEADER_BYTE => DType::TimestampMicros,
      TimestampMicros96::HEADER_BYTE => DType::TimestampMicros96,
      TimestampNanos::HEADER_BYTE => DType::TimestampNanos,
      TimestampNanos96::HEADER_BYTE => DType::TimestampNanos96,
      u16::HEADER_BYTE => DType::U16,
      u32::HEADER_BYTE => DType::U32,
      u64::HEADER_BYTE => DType::U64,
      u128::HEADER_BYTE => DType::U128,
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
      DType::I16 => ArrowDataType::Int16,
      DType::I32 => ArrowDataType::Int32,
      DType::I64 => ArrowDataType::Int64,
      DType::U16 => ArrowDataType::UInt16,
      DType::U32 => ArrowDataType::UInt32,
      DType::U64 => ArrowDataType::UInt64,
      DType::TimestampMicros => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
      DType::TimestampNanos => ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
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
      ArrowDataType::Int16 => DType::I16,
      ArrowDataType::Int32 => DType::I32,
      ArrowDataType::Int64 => DType::I64,
      ArrowDataType::UInt16 => DType::U16,
      ArrowDataType::UInt32 => DType::U32,
      ArrowDataType::UInt64 => DType::U64,
      ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => DType::TimestampMicros,
      ArrowDataType::Timestamp(TimeUnit::Nanosecond, _) => DType::TimestampNanos,
      _ => {
        return Err(anyhow!("unable to convert arrow dtype {:?} to q_compress", arrow_dtype))
      }
    };
    Ok(res)
  }
}

#[cfg(test)]
mod tests {
  use std::str::FromStr;
  use anyhow::Result;
  use enum_iterator::IntoEnumIterator;

  use crate::dtype::DType;

  #[test]
  fn test_arrow_dtypes_consistent() -> Result<()> {
    for dtype in DType::into_enum_iter() {
      if let Ok(arrow_dtype) = dtype.to_arrow() {
        assert_eq!(DType::from_arrow(&arrow_dtype)?, dtype);
      }
    }
    Ok(())
  }

  #[test]
  fn test_dtype_nameable() -> Result<()> {
    for dtype in DType::into_enum_iter() {
      let name = format!("{:?}", dtype);
      let recovered = DType::from_str(&name)?;
      assert_eq!(recovered, dtype);
    }
    Ok(())
  }
}
