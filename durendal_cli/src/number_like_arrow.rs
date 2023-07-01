use arrow::datatypes as arrow_dtypes;
use arrow::datatypes::ArrowPrimitiveType;

use durendal::data_types::NumberLike;

pub trait NumberLikeArrow: ArrowPrimitiveType {
  type Num: NumberLike;

  fn native_to_num(native: Self::Native) -> Self::Num;
  fn num_to_native(num: Self::Num) -> Self::Native;
}

macro_rules! trivial_arrow {
  ($t: ty, $p: ty) => {
    impl NumberLikeArrow for $p {
      type Num = $t;

      fn native_to_num(native: Self::Native) -> Self::Num {
        native
      }

      fn num_to_native(num: Self::Num) -> Self::Native {
        num
      }
    }
  };
}

trivial_arrow!(f32, arrow_dtypes::Float32Type);
trivial_arrow!(f64, arrow_dtypes::Float64Type);
trivial_arrow!(i32, arrow_dtypes::Int32Type);
trivial_arrow!(i64, arrow_dtypes::Int64Type);
trivial_arrow!(i64, arrow_dtypes::TimestampMicrosecondType);
trivial_arrow!(i64, arrow_dtypes::TimestampNanosecondType);
trivial_arrow!(u32, arrow_dtypes::UInt32Type);
trivial_arrow!(u64, arrow_dtypes::UInt64Type);

impl NumberLikeArrow for arrow_dtypes::Int16Type {
  type Num = i32;

  fn native_to_num(native: Self::Native) -> Self::Num {
    native as i32
  }

  fn num_to_native(num: Self::Num) -> Self::Native {
    num as i16
  }
}

impl NumberLikeArrow for arrow_dtypes::UInt16Type {
  type Num = u32;

  fn native_to_num(native: Self::Native) -> Self::Num {
    native as u32
  }

  fn num_to_native(num: Self::Num) -> Self::Native {
    num as u16
  }
}
