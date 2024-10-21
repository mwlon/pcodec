use crate::data_types::Number;
use crate::macros::define_number_enum;

define_number_enum!(
  #[derive(Clone, Copy, Debug, PartialEq, Eq)]
  #[repr(u8)]
  pub NumberType = NUMBER_TYPE_BYTE
);
