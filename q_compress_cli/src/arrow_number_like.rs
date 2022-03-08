use arrow::datatypes as arrow_dtypes;
use arrow::datatypes::ArrowPrimitiveType;

use q_compress::data_types::{NumberLike, TimestampMicros, TimestampNanos};

pub trait ArrowNumberLike: NumberLike {
  const IS_ARROW: bool;
  type ArrowPrimitive: ArrowPrimitiveType;

  fn from_arrow(native: <Self::ArrowPrimitive as ArrowPrimitiveType>::Native) -> Self;
  
  fn to_arrow(self) -> <Self::ArrowPrimitive as ArrowPrimitiveType>::Native;
}

macro_rules! trivial_arrow {
  ($t: ty, $p: ty) => {
    impl ArrowNumberLike for $t {
      const IS_ARROW: bool = true;
      type ArrowPrimitive = $p;

      fn from_arrow(native: Self) -> Self {
        native
      }
      
      fn to_arrow(self) -> Self {
        self
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
      
      fn to_arrow(self) -> f32 {
        unreachable!()
      }
    }
  }
}

no_arrow!(bool);
no_arrow!(i128);
no_arrow!(u128);
trivial_arrow!(f32, arrow_dtypes::Float32Type);
trivial_arrow!(f64, arrow_dtypes::Float64Type);
trivial_arrow!(i16, arrow_dtypes::Int16Type);
trivial_arrow!(i32, arrow_dtypes::Int32Type);
trivial_arrow!(i64, arrow_dtypes::Int64Type);
trivial_arrow!(u16, arrow_dtypes::UInt16Type);
trivial_arrow!(u32, arrow_dtypes::UInt32Type);
trivial_arrow!(u64, arrow_dtypes::UInt64Type);

impl ArrowNumberLike for TimestampMicros {
  const IS_ARROW: bool = true;
  type ArrowPrimitive = arrow_dtypes::TimestampMicrosecondType;

  fn from_arrow(native: i64) -> Self {
    TimestampMicros::new(native as i128).unwrap()
  }

  fn to_arrow(self) -> i64 {
    self.to_total_parts() as i64
  }
}

impl ArrowNumberLike for TimestampNanos {
  const IS_ARROW: bool = true;
  type ArrowPrimitive = arrow_dtypes::TimestampNanosecondType;

  fn from_arrow(native: i64) -> Self {
    TimestampNanos::new(native as i128).unwrap()
  }

  fn to_arrow(self) -> <Self::ArrowPrimitive as ArrowPrimitiveType>::Native {
    self.to_total_parts() as i64
  }
}
