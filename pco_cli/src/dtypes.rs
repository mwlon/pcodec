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

  fn native_vec_to_pco(native: Vec<Self::Native>) -> Vec<Self::Pco>;
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

      fn native_vec_to_pco(native: Vec<Self::Native>) -> Vec<Self::Pco> {
        native
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

      fn native_vec_to_pco(native: Vec<Self::Native>) -> Vec<Self::Pco> {
        native
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
extra_arrow!(i32, arrow_dtypes::Int16Type);
extra_arrow!(u32, arrow_dtypes::UInt16Type);
extra_arrow!(i64, arrow_dtypes::TimestampMicrosecondType);
extra_arrow!(i64, arrow_dtypes::TimestampNanosecondType);

pub fn from_arrow(arrow_dtype: &ArrowDataType) -> Result<CoreDataType> {
  let res = match arrow_dtype {
    ArrowDataType::Float32 => CoreDataType::F32,
    ArrowDataType::Float64 => CoreDataType::F64,
    ArrowDataType::Int16 => CoreDataType::I32,
    ArrowDataType::Int32 => CoreDataType::I32,
    ArrowDataType::Int64 => CoreDataType::I64,
    ArrowDataType::UInt16 => CoreDataType::U32,
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