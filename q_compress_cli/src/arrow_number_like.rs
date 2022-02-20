use arrow::datatypes as arrow_dtypes;

use q_compress::data_types::{NumberLike, TimestampMicros, TimestampNanos};

pub trait ArrowNumberLike: NumberLike {
  const IS_ARROW: bool;
  type ArrowPrimitive: arrow_dtypes::ArrowPrimitiveType;

  fn from_arrow(native: <Self::ArrowPrimitive as arrow_dtypes::ArrowPrimitiveType>::Native) -> Self;
}

macro_rules! trivial_arrow {
  ($t: ty, $p: ty) => {
    impl ArrowNumberLike for $t {
      const IS_ARROW: bool = true;
      type ArrowPrimitive = $p;

      fn from_arrow(native: Self) -> Self {
        native
      }
    }
  }
}

macro_rules! no_arrow {
  ($t: ty) => {
    impl ArrowNumberLike for $t {
      const IS_ARROW: bool = false;
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

impl ArrowNumberLike for TimestampMicros {
  const IS_ARROW: bool = true;
  type ArrowPrimitive = arrow_dtypes::TimestampMicrosecondType;

  fn from_arrow(native: i64) -> Self {
    TimestampMicros::new(native as i128).unwrap()
  }
}

impl ArrowNumberLike for TimestampNanos {
  const IS_ARROW: bool = true;
  type ArrowPrimitive = arrow_dtypes::TimestampNanosecondType;

  fn from_arrow(native: i64) -> Self {
    TimestampNanos::new(native as i128).unwrap()
  }
}
