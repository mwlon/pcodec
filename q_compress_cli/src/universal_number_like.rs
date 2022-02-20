use arrow::datatypes as arrow_dtypes;

use q_compress::data_types::{NumberLike, TimestampMicros, TimestampNanos};

pub trait ArrowLike {
  const SUPPORTED: bool;
  type ArrowPrimitive: arrow_dtypes::ArrowPrimitiveType;

  fn from_arrow(native: <Self::ArrowPrimitive as arrow_dtypes::ArrowPrimitiveType>::Native) -> Self;
}

pub trait UniversalNumberLike: NumberLike + ArrowLike {}

// macro_rules! trivial_parquet {
//   ($t: ty, $p: ty) => {
//     impl ParquetLike for $t {
//       const SUPPORTED: bool = true;
//       type ParquetDType = $p;
//
//       fn from_parquet(native: Self) -> Self {
//         native
//       }
//     }
//   }
// }

macro_rules! trivial_arrow {
  ($t: ty, $p: ty) => {
    impl ArrowLike for $t {
      const SUPPORTED: bool = true;
      type ArrowPrimitive = $p;

      fn from_arrow(native: Self) -> Self {
        native
      }
    }
  }
}

// macro_rules! no_parquet {
//   ($t: ty) => {
//     impl ParquetLike for $t {
//       const SUPPORTED: bool = false;
//       type ParquetDType = parquet_dtypes::BoolType; // fake
//
//       fn from_parquet(_: bool) -> Self {
//         unreachable!()
//       }
//     }
//   }
// }

macro_rules! no_arrow {
  ($t: ty) => {
    impl ArrowLike for $t {
      const SUPPORTED: bool = false;
      type ArrowPrimitive = arrow_dtypes::Float32Type; // fake

      fn from_arrow(_: f32) -> Self {
        unreachable!()
      }
    }
  }
}

no_arrow!(bool);
no_arrow!(i128);
trivial_arrow!(f32, arrow_dtypes::Float32Type);
trivial_arrow!(f64, arrow_dtypes::Float64Type);
trivial_arrow!(i32, arrow_dtypes::Int32Type);
trivial_arrow!(i64, arrow_dtypes::Int64Type);
trivial_arrow!(u32, arrow_dtypes::UInt32Type);
trivial_arrow!(u64, arrow_dtypes::UInt64Type);
//
// no_parquet!(i128);
// no_parquet!(u32);
// no_parquet!(u64);
// no_parquet!(TimestampNanos);
// trivial_parquet!(bool, parquet_dtypes::BoolType);
// trivial_parquet!(f32, parquet_dtypes::FloatType);
// trivial_parquet!(f64, parquet_dtypes::DoubleType);
// trivial_parquet!(i32, parquet_dtypes::Int32Type);
// trivial_parquet!(i64, parquet_dtypes::Int64Type);

impl ArrowLike for TimestampMicros {
  const SUPPORTED: bool = true;
  type ArrowPrimitive = arrow_dtypes::TimestampMicrosecondType;

  fn from_arrow(native: i64) -> Self {
    TimestampMicros::new(native as i128).unwrap()
  }
}

impl ArrowLike for TimestampNanos {
  const SUPPORTED: bool = true;
  type ArrowPrimitive = arrow_dtypes::TimestampNanosecondType;

  fn from_arrow(native: i64) -> Self {
    TimestampNanos::new(native as i128).unwrap()
  }
}

// impl ParquetLike for TimestampMicros {
//   const SUPPORTED: bool = true;
//   type ParquetDType = parquet_dtypes::Int96Type;
//
//   fn from_parquet(native: parquet_dtypes::Int96) -> Self {
//     let data = native.data();
//     let mut signed = 0_i128;
//     for i in 0..3 {
//       signed <<= 32;
//       signed += data[i] as i128;
//     }
//     TimestampMicros::new(signed).unwrap()
//   }
// }

impl UniversalNumberLike for bool {}
impl UniversalNumberLike for f32 {}
impl UniversalNumberLike for f64 {}
impl UniversalNumberLike for i32 {}
impl UniversalNumberLike for i64 {}
impl UniversalNumberLike for i128 {}
impl UniversalNumberLike for TimestampMicros {}
impl UniversalNumberLike for TimestampNanos {}
impl UniversalNumberLike for u32 {}
impl UniversalNumberLike for u64 {}
