use crate::data_types::NumberLike;

macro_rules! impl_signed {
  ($t: ty, $unsigned: ty, $header_byte: expr) => {
    impl NumberLike for $t {
      const DTYPE_BYTE: u8 = $header_byte;

      type Unsigned = $unsigned;

      #[inline]
      fn to_unsigned(self) -> Self::Unsigned {
        self.wrapping_sub(Self::MIN) as $unsigned
      }

      #[inline]
      fn from_unsigned(off: Self::Unsigned) -> Self {
        Self::MIN.wrapping_add(off as $t)
      }

      #[inline]
      fn transmute_to_unsigned_slice(slice: &mut [Self]) -> &mut [Self::Unsigned] {
        unsafe { std::mem::transmute(slice) }
      }

      #[inline]
      fn transmute_to_unsigned(self) -> Self::Unsigned {
        unsafe { std::mem::transmute(self) }
      }
    }
  };
}

impl_signed!(i32, u32, 3);
impl_signed!(i64, u64, 4);
