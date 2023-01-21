use std::convert::TryInto;

use crate::data_types::NumberLike;
use crate::errors::QCompressResult;

// Note that in all conversions between float and unsigned int, we are using
// the unsigned int to indicate an offset.
// For instance, since f32 has 23 fraction bits, here we want 1.0 + 3_u32 to be
// 1.0 + (3.0 * 2.0 ^ -23).
macro_rules! impl_float_number {
  ($t: ty, $signed: ty, $unsigned: ty, $bits: expr, $sign_bit_mask: expr, $header_byte: expr) => {
    impl NumberLike for $t {
      const HEADER_BYTE: u8 = $header_byte;
      const PHYSICAL_BITS: usize = $bits;

      type Signed = $signed;
      type Unsigned = $unsigned;

      // miraculously, this should preserve ordering
      fn to_signed(self) -> Self::Signed {
        self.to_bits() as Self::Signed
      }

      fn from_signed(signed: Self::Signed) -> Self {
        Self::from_bits(signed as Self::Unsigned)
      }

      fn to_unsigned(self) -> Self::Unsigned {
        let mem_layout = self.to_bits();
        if mem_layout & $sign_bit_mask > 0 {
          // negative float
          !mem_layout
        } else {
          // positive float
          mem_layout ^ $sign_bit_mask
        }
      }

      fn from_unsigned(off: Self::Unsigned) -> Self {
        if off & $sign_bit_mask > 0 {
          // positive float
          Self::from_bits(off ^ $sign_bit_mask)
        } else {
          // negative float
          Self::from_bits(!off)
        }
      }

      fn to_bytes(self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
      }

      fn from_bytes(bytes: &[u8]) -> QCompressResult<Self> {
        Ok(Self::from_be_bytes(bytes.try_into().unwrap()))
      }
    }
  }
}

impl_float_number!(f32, i32, u32, 32, 1_u32 << 31, 6);
impl_float_number!(f64, i64, u64, 64, 1_u64 << 63, 5);
