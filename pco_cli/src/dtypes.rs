use std::mem;

use anyhow::anyhow;
use anyhow::Result;
use arrow::datatypes as arrow_dtypes;
use arrow::datatypes::DataType as ArrowDataType;
use arrow::datatypes::{ArrowPrimitiveType, DataType};

use half::f16;
use pco::data_types::{CoreDataType, NumberLike};

use crate::num_vec::NumVec;

pub trait Parquetable: Sized {
  const PARQUET_DTYPE_STR: &'static str;

  type Parquet: parquet::data_type::DataType;

  fn nums_to_parquet(nums: &[Self]) -> &[<Self::Parquet as parquet::data_type::DataType>::T];
  fn parquet_to_nums(vec: Vec<<Self::Parquet as parquet::data_type::DataType>::T>) -> Vec<Self>;
}

#[cfg(feature = "full_bench")]
pub trait QCompressable: Sized {
  type Qco: q_compress::data_types::NumberLike;

  fn nums_to_qco(nums: &[Self]) -> &[Self::Qco];
  fn qco_to_nums(vec: Vec<Self::Qco>) -> Vec<Self>;
}

#[cfg(feature = "full_bench")]
pub trait PcoNumberLike: NumberLike + Parquetable + QCompressable {
  const ARROW_DTYPE: DataType;

  type Arrow: ArrowPrimitiveType;

  fn to_arrow_native(self) -> <Self::Arrow as ArrowPrimitiveType>::Native;
  fn make_num_vec(nums: Vec<Self>) -> NumVec;
  fn arrow_native_to_bytes(x: <Self::Arrow as ArrowPrimitiveType>::Native) -> Vec<u8>;
}

#[cfg(not(feature = "full_bench"))]
pub trait PcoNumberLike: NumberLike + Parquetable {
  const ARROW_DTYPE: DataType;

  type Arrow: ArrowPrimitiveType;

  fn to_arrow_native(self) -> <Self::Arrow as ArrowPrimitiveType>::Native;
  fn make_num_vec(nums: Vec<Self>) -> NumVec;
  fn arrow_native_to_bytes(x: <Self::Arrow as ArrowPrimitiveType>::Native) -> Vec<u8>;
}

pub trait ArrowNumberLike: ArrowPrimitiveType {
  type Pco: PcoNumberLike;

  fn native_to_pco(native: Self::Native) -> Self::Pco;

  fn native_vec_to_pco(native: Vec<Self::Native>) -> Vec<Self::Pco>;
}

macro_rules! parquetable {
  ($t: ty, $parq: ty, $parq_str: expr) => {
    impl Parquetable for $t {
      const PARQUET_DTYPE_STR: &'static str = $parq_str;

      type Parquet = $parq;

      fn nums_to_parquet(nums: &[Self]) -> &[<Self::Parquet as parquet::data_type::DataType>::T] {
        nums
      }
      fn parquet_to_nums(
        vec: Vec<<Self::Parquet as parquet::data_type::DataType>::T>,
      ) -> Vec<Self> {
        vec
      }
    }
  };
}

macro_rules! trivial {
  ($t: ty, $name: ident, $p: ty) => {
    #[cfg(feature = "full_bench")]
    impl QCompressable for $t {
      type Qco = $t;

      fn nums_to_qco(nums: &[Self]) -> &[Self::Qco] {
        nums
      }
      fn qco_to_nums(vec: Vec<Self::Qco>) -> Vec<Self> {
        vec
      }
    }

    impl PcoNumberLike for $t {
      const ARROW_DTYPE: DataType = <$p as ArrowPrimitiveType>::DATA_TYPE;

      type Arrow = $p;

      fn to_arrow_native(self) -> <Self::Arrow as ArrowPrimitiveType>::Native {
        self as Self
      }

      fn make_num_vec(nums: Vec<Self>) -> NumVec {
        NumVec::$name(nums)
      }

      fn arrow_native_to_bytes(x: <Self::Arrow as ArrowPrimitiveType>::Native) -> Vec<u8> {
        x.to_le_bytes().to_vec()
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

parquetable!(f32, parquet::data_type::FloatType, "FLOAT");
parquetable!(f64, parquet::data_type::DoubleType, "DOUBLE");
parquetable!(i32, parquet::data_type::Int32Type, "INT32");
parquetable!(i64, parquet::data_type::Int64Type, "INT64");

impl Parquetable for f16 {
  const PARQUET_DTYPE_STR: &'static str = "FLOAT";
  type Parquet = parquet::data_type::FloatType;

  // Parquet doesn't have unsigned integer types, so the best zero-copy thing
  // we can do is transmute to signed ones.
  fn nums_to_parquet(nums: &[Self]) -> &[<Self::Parquet as parquet::data_type::DataType>::T] {
    todo!()
  }
  fn parquet_to_nums(vec: Vec<<Self::Parquet as parquet::data_type::DataType>::T>) -> Vec<Self> {
    todo!()
  }
}

impl Parquetable for i16 {
  const PARQUET_DTYPE_STR: &'static str = "INT32";
  type Parquet = parquet::data_type::Int32Type;

  // Parquet doesn't have unsigned integer types, so the best zero-copy thing
  // we can do is transmute to signed ones.
  fn nums_to_parquet(nums: &[Self]) -> &[<Self::Parquet as parquet::data_type::DataType>::T] {
    todo!()
  }
  fn parquet_to_nums(vec: Vec<<Self::Parquet as parquet::data_type::DataType>::T>) -> Vec<Self> {
    todo!()
  }
}

impl Parquetable for u16 {
  const PARQUET_DTYPE_STR: &'static str = "INT32";
  type Parquet = parquet::data_type::Int32Type;

  // Parquet doesn't have unsigned integer types, so the best zero-copy thing
  // we can do is transmute to signed ones.
  fn nums_to_parquet(nums: &[Self]) -> &[<Self::Parquet as parquet::data_type::DataType>::T] {
    todo!()
  }
  fn parquet_to_nums(vec: Vec<<Self::Parquet as parquet::data_type::DataType>::T>) -> Vec<Self> {
    todo!()
  }
}

// TODO: verify implementation
impl Parquetable for u32 {
  const PARQUET_DTYPE_STR: &'static str = "INT32";
  type Parquet = parquet::data_type::Int32Type;

  // Parquet doesn't have unsigned integer types, so the best zero-copy thing
  // we can do is transmute to signed ones.
  fn nums_to_parquet(nums: &[Self]) -> &[<Self::Parquet as parquet::data_type::DataType>::T] {
    unsafe { mem::transmute(nums) }
  }
  fn parquet_to_nums(vec: Vec<<Self::Parquet as parquet::data_type::DataType>::T>) -> Vec<Self> {
    unsafe { mem::transmute(vec) }
  }
}

impl Parquetable for u64 {
  const PARQUET_DTYPE_STR: &'static str = "INT64";
  type Parquet = parquet::data_type::Int64Type;

  // Parquet doesn't have unsigned integer types, so the best zero-copy thing
  // we can do is transmute to signed ones.
  fn nums_to_parquet(nums: &[Self]) -> &[<Self::Parquet as parquet::data_type::DataType>::T] {
    unsafe { mem::transmute(nums) }
  }
  fn parquet_to_nums(vec: Vec<<Self::Parquet as parquet::data_type::DataType>::T>) -> Vec<Self> {
    unsafe { mem::transmute(vec) }
  }
}

trivial!(f16, F16, arrow_dtypes::Float16Type);
trivial!(f32, F32, arrow_dtypes::Float32Type);
trivial!(f64, F64, arrow_dtypes::Float64Type);
trivial!(i16, I16, arrow_dtypes::Int16Type);
trivial!(i32, I32, arrow_dtypes::Int32Type);
trivial!(i64, I64, arrow_dtypes::Int64Type);
trivial!(u16, U16, arrow_dtypes::UInt16Type);
trivial!(u32, U32, arrow_dtypes::UInt32Type);
trivial!(u64, U64, arrow_dtypes::UInt64Type);

extra_arrow!(i64, arrow_dtypes::TimestampSecondType);
extra_arrow!(i64, arrow_dtypes::TimestampMillisecondType);
extra_arrow!(i64, arrow_dtypes::TimestampMicrosecondType);
extra_arrow!(i64, arrow_dtypes::TimestampNanosecondType);

pub fn from_arrow(arrow_dtype: &ArrowDataType) -> Result<CoreDataType> {
  let res = match arrow_dtype {
    ArrowDataType::Float16 => CoreDataType::F16,
    ArrowDataType::Float32 => CoreDataType::F32,
    ArrowDataType::Float64 => CoreDataType::F64,
    ArrowDataType::Int16 => CoreDataType::I16,
    ArrowDataType::Int32 => CoreDataType::I32,
    ArrowDataType::Int64 => CoreDataType::I64,
    ArrowDataType::UInt16 => CoreDataType::U16,
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

pub fn to_arrow(dtype: CoreDataType) -> ArrowDataType {
  match dtype {
    CoreDataType::F16 => ArrowDataType::Float16,
    CoreDataType::F32 => ArrowDataType::Float32,
    CoreDataType::F64 => ArrowDataType::Float64,
    CoreDataType::I16 => ArrowDataType::Int16,
    CoreDataType::I32 => ArrowDataType::Int32,
    CoreDataType::I64 => ArrowDataType::Int64,
    CoreDataType::U16 => ArrowDataType::UInt16,
    CoreDataType::U32 => ArrowDataType::UInt32,
    CoreDataType::U64 => ArrowDataType::UInt64,
  }
}
