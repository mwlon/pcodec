use CoreDataType::*;

use crate::data_types::NumberLike;

macro_rules! impl_core_dtypes {
  {$($name:ident($uname:ident) => $t:ty,)+} => {
    /// A dynamic value representing one of the core data types implemented in
    /// pco.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    #[repr(u8)]
    pub enum CoreDataType { $($name = <$t>::DTYPE_BYTE,)+ }

    impl CoreDataType {
      pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
          $(<$t>::DTYPE_BYTE => Some($name),)+
          _ => None,
        }
      }
    }
  };
}

macro_rules! impl_core_unsigneds {
  {$($name:ident => $t:ty,)+} => {
    /// A dynamic value representing one of the core unsigned data types that
    /// can be used for latents in pco.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum CoreUnsignedType { $($name,)+ }
  };
}

#[macro_export]
macro_rules! with_core_dtypes {
  ($inner:tt) => {
    $inner!(
      U32(U32) => u32,
      U64(U64) => u64,
      I32(U32) => i32,
      I64(U64) => i64,
      F32(U32) => f32,
      F64(U64) => f64,
    );
  }
}

#[macro_export]
macro_rules! with_core_unsigneds {
  ($inner:tt) => {
    $inner!(
      U32 => u32,
      U64 => u64,
    );
  }
}

with_core_dtypes!(impl_core_dtypes);
with_core_unsigneds!(impl_core_unsigneds);
