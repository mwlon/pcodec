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

/// A macro to help cross the dynamic<->generic boundary for pco core data
/// types.
///
/// Accepts a macro of a particular format. For example:
/// ```
/// use pco::data_types::{CoreDataType, NumberLike, Latent};
/// use pco::with_core_dtypes;
/// fn generic_fn<T: NumberLike>() -> String {
///   T::default().to_string()
/// }
///
///
/// let dtype = CoreDataType::U32;
/// macro_rules! get_dtype_byte {
///   {$($name:ident($uname:ident) => $t:ty,)+} => {
///     match dtype {
///       $(CoreDataType::$name => generic_fn::<$t>(),)+
///     }
///   }
/// }
/// let output = with_core_dtypes!(get_dtype_byte);
/// println!("generic_fn run on {:?}: {}", dtype, output)
/// ```
#[macro_export]
macro_rules! with_core_dtypes {
  ($inner:ident) => {
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

/// Similar to with_core_dtypes, but only for core unsigned types.
/// Accepts a macro over a repeated list of `$($name => $t,)+`.
#[macro_export]
macro_rules! with_core_unsigneds {
  ($inner:ident) => {
    $inner!(
      U32 => u32,
      U64 => u64,
    );
  }
}

with_core_dtypes!(impl_core_dtypes);
