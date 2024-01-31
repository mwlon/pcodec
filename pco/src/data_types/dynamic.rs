use CoreDataType::*;

use crate::data_types::NumberLike;

macro_rules! impl_core_dtypes {
  {$($names:ident => $types:ty,)+} => {
    /// A dynamic value representing one of the core data types implemented in
    /// pco.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    #[repr(u8)]
    pub enum CoreDataType { $($names = <$types>::DTYPE_BYTE,)+ }

    impl CoreDataType {
      pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
          $(<$types>::DTYPE_BYTE => Some($names),)+
          _ => None,
        }
      }
    }

    pub macro_rules! dyn_typed_enum {
      ($name:ty, $($wrapped_ty:ty)?) => {
        $($names$((<$wrapped_ty>::<$types>))?)+
      }
    }
  };
}

impl_core_dtypes!(
  U32 => u32,
  U64 => u64,
  I32 => i32,
  I64 => i64,
  F32 => f32,
  F64 => f64,
);

dyn_typed_enum!(Asdf);
dyn_typed_enum!(Asdf2, Vec);
