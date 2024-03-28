use anyhow::anyhow;
use anyhow::Result;
use arrow::datatypes as arrow_dtypes;
use arrow::datatypes::DataType as ArrowDataType;
use arrow::datatypes::{ArrowPrimitiveType, DataType};

use pco::data_types::{CoreDataType, NumberLike};

pub trait PcoNumberLike: NumberLike {
  const ARROW_DTYPE: DataType;

  type Arrow: ArrowPrimitiveType;

  fn to_arrow_native(self) -> <Self::Arrow as ArrowPrimitiveType>::Native;
}

pub trait ArrowNumberLike: ArrowPrimitiveType {
  type Pco: PcoNumberLike;

  fn native_to_pco(native: Self::Native) -> Self::Pco;
}

macro_rules! trivial {
  ($t: ty, $p: ty) => {
    impl PcoNumberLike for $t {
      const ARROW_DTYPE: DataType = <$p as ArrowPrimitiveType>::DATA_TYPE;

      type Arrow = $p;

      fn to_arrow_native(self) -> <Self::Arrow as ArrowPrimitiveType>::Native {
        self as Self
      }
    }

    impl ArrowNumberLike for $p {
      type Pco = $t;

      fn native_to_pco(native: Self::Native) -> Self::Pco {
        native as Self::Pco
      }
    }
  };
}

macro_rules! extra_arrow {
  ($t: ty, $p: ty) => {
    impl ArrowNumberLike for $p {
      type Pco = $t;

      fn native_to_pco(native: Self::Native) -> Self::Pco {
        native as Self::Pco
      }
    }
  };
}

trivial!(f32, arrow_dtypes::Float32Type);
trivial!(f64, arrow_dtypes::Float64Type);
trivial!(i32, arrow_dtypes::Int32Type);
trivial!(i64, arrow_dtypes::Int64Type);
trivial!(u32, arrow_dtypes::UInt32Type);
trivial!(u64, arrow_dtypes::UInt64Type);
extra_arrow!(i64, arrow_dtypes::TimestampMicrosecondType);
extra_arrow!(i64, arrow_dtypes::TimestampNanosecondType);

pub fn to_arrow(dtype: CoreDataType) -> ArrowDataType {
  match dtype {
    CoreDataType::F32 => ArrowDataType::Float32,
    CoreDataType::F64 => ArrowDataType::Float64,
    CoreDataType::I32 => ArrowDataType::Int32,
    CoreDataType::I64 => ArrowDataType::Int64,
    CoreDataType::U32 => ArrowDataType::UInt32,
    CoreDataType::U64 => ArrowDataType::UInt64,
  }
}

pub fn from_arrow(arrow_dtype: &ArrowDataType) -> Result<CoreDataType> {
  let res = match arrow_dtype {
    ArrowDataType::Float32 => CoreDataType::F32,
    ArrowDataType::Float64 => CoreDataType::F64,
    ArrowDataType::Int32 => CoreDataType::I32,
    ArrowDataType::Int64 => CoreDataType::I64,
    ArrowDataType::UInt32 => CoreDataType::U32,
    ArrowDataType::UInt64 => CoreDataType::U64,
    ArrowDataType::Timestamp(_, _) => CoreDataType::I64,
    _ => {
      return Err(anyhow!(
        "unable to convert arrow dtype {:?} to pco",
        arrow_dtype
      ))
    }
  };
  Ok(res)
}

#[cfg(test)]
mod tests {
  use anyhow::Result;

  use pco::data_types::CoreDataType;
  use pco::with_core_dtypes;

  use super::*;

  #[test]
  fn test_arrow_dtypes_consistent() -> Result<()> {
    use CoreDataType::*;
    macro_rules! check_dtype {
      {$($name:ident($lname:ident) => $t:ty,)+} => {
        $(assert_eq!(from_arrow(&to_arrow($name))?, $name);)+
      }
    }
    with_core_dtypes!(check_dtype);
    Ok(())
  }
}
