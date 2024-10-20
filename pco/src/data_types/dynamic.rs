use crate::data_types::NumberLike;
use crate::macros::define_number_like_enum;

define_number_like_enum!(
  #[derive(Clone, Copy, Debug, PartialEq, Eq)]
  #[repr(u8)]
  pub CoreDataType = DTYPE_BYTE
);
