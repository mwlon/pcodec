use std::convert::TryFrom;
use std::str::FromStr;

use anyhow::anyhow;
use anyhow::Result;
use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};

use durendal::data_types::{
  NumberLike
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(test, derive(enum_iterator::IntoEnumIterator))]
pub enum DType {
  F32,
  F64,
  I32,
  I64,
  TimestampMicros,
  TimestampNanos,
  U32,
  U64,
}

impl FromStr for DType {
  type Err = anyhow::Error;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    let res = match s.to_lowercase().as_str() {
      "f32" => DType::F32,
      "f64" => DType::F64,
      "i32" => DType::I32,
      "i64" => DType::I64,
      "u32" => DType::U32,
      "u64" => DType::U64,
      "micros" | "timestampmicros" => DType::TimestampMicros,
      "nanos" | "timestampnanos" => DType::TimestampNanos,
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
      f32::HEADER_BYTE => DType::F32,
      f64::HEADER_BYTE => DType::F64,
      i32::HEADER_BYTE => DType::I32,
      i64::HEADER_BYTE => DType::I64,
      u32::HEADER_BYTE => DType::U32,
      u64::HEADER_BYTE => DType::U64,
      _ => {
        return Err(anyhow!(
          "unknown data type byte {}",
          header_byte
        ));
      }
    };
    Ok(res)
  }
}

impl DType {
  pub fn to_arrow(self) -> Result<ArrowDataType> {
    let res = match self {
      DType::F32 => ArrowDataType::Float32,
      DType::F64 => ArrowDataType::Float64,
      DType::I32 => ArrowDataType::Int32,
      DType::I64 => ArrowDataType::Int64,
      DType::U32 => ArrowDataType::UInt32,
      DType::U64 => ArrowDataType::UInt64,
      DType::TimestampMicros => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
      DType::TimestampNanos => ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
    };
    Ok(res)
  }

  pub fn from_arrow(arrow_dtype: &ArrowDataType) -> Result<Self> {
    let res = match arrow_dtype {
      ArrowDataType::Float32 => DType::F32,
      ArrowDataType::Float64 => DType::F64,
      ArrowDataType::Int32 => DType::I32,
      ArrowDataType::Int64 => DType::I64,
      ArrowDataType::UInt32 => DType::U32,
      ArrowDataType::UInt64 => DType::U64,
      ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => DType::TimestampMicros,
      ArrowDataType::Timestamp(TimeUnit::Nanosecond, _) => DType::TimestampNanos,
      _ => {
        return Err(anyhow!(
          "unable to convert arrow dtype {:?} to durendal",
          arrow_dtype
        ))
      }
    };
    Ok(res)
  }
}

#[cfg(test)]
mod tests {
  use anyhow::Result;
  use enum_iterator::IntoEnumIterator;
  use std::str::FromStr;

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
