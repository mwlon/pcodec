use crate::data_types::{NumberLike, SignedLike};

macro_rules! impl_signed {
  ($t: ty, $unsigned: ty, $header_byte: expr) => {
    impl SignedLike for $t {
      const ZERO: Self = 0;

      #[inline]
      fn wrapping_add(self, other: Self) -> Self {
        self.wrapping_add(other)
      }

      #[inline]
      fn wrapping_sub(self, other: Self) -> Self {
        self.wrapping_sub(other)
      }
    }

    impl NumberLike for $t {
      const HEADER_BYTE: u8 = $header_byte;
      const PHYSICAL_BITS: usize = Self::BITS as usize;

      type Signed = Self;
      type Unsigned = $unsigned;

      #[inline]
      fn to_signed(self) -> Self::Signed {
        self
      }

      #[inline]
      fn from_signed(signed: Self::Signed) -> Self {
        signed
      }

      #[inline]
      fn to_unsigned(self) -> Self::Unsigned {
        self.wrapping_sub(Self::MIN) as $unsigned
      }

      #[inline]
      fn from_unsigned(off: Self::Unsigned) -> Self {
        Self::MIN.wrapping_add(off as $t)
      }
    }
  };
}

impl_signed!(i32, u32, 3);
impl_signed!(i64, u64, 1);
