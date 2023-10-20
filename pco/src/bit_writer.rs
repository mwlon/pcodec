use crate::bits;
use crate::constants::{Bitlen, BYTES_PER_WORD, WORD_BITLEN, WORD_SIZE};
use crate::data_types::UnsignedLike;
use crate::errors::{PcoError, PcoResult};
use crate::read_write_uint::ReadWriteUint;

#[derive(Clone, Debug, Default)]
pub struct BitWriter<'a> {
  dst: &'a mut [u8],
  pub byte_idx: usize,
  pub bits_past_byte: Bitlen,
}

impl<'a> From<&'a mut [u8]> for BitWriter<'a> {
  fn from(dst: &'a mut [u8]) -> Self {
    Self {
      dst,
      byte_idx: 0,
      bits_past_byte: 0,
    }
  }
}

impl<'a> BitWriter {
  #[inline]
  fn refill(&mut self) {
    self.byte_idx += self.bits_past_byte / 8;
    self.bits_past_byte %= 8;
  }

  #[inline]
  fn consume(&mut self, n: Bitlen) {
    self.bits_past_byte += n;
  }

  pub fn write_aligned_bytes(&mut self, bytes: &[u8]) -> PcoResult<()> {
    if self.bits_past_byte % 8 == 0 {
      self.refill();

      let end = bytes.len() + self.byte_idx;
      if end > self.dst.len() {
        return Err(PcoError::insufficient_data(format!(
          "cannot write {} more bytes with at byte {}/{}",
          bytes.len(),
          self.byte_idx,
          self.dst.len(),
        )))
      }
      self.dst[self.byte_idx..end].clone_from_slice(bytes);

      self.consume((bytes.len() * 8) as Bitlen);
      Ok(())
    } else {
      Err(PcoError::invalid_argument(format!(
        "cannot write aligned bytes to unaligned writer (bit idx {})",
        self.bits_past_byte,
      )))
    }
  }

  pub fn write_usize(&mut self, mut x: usize, n: Bitlen) -> PcoResult<()> {
    self.refill();
    self.write_uint_at(self.byte_idx, self.bits_past_byte, x, n)?;
    self.consume(n);
    Ok(())
  }

  pub fn write_bitlen(&mut self, x: Bitlen, n: Bitlen) -> PcoResult<()> {
    self.write_uint_at(self.byte_idx, self.bits_past_byte, x, n)
  }

  fn write_uint_at<U: ReadWriteUint>(&mut self, mut i: usize, mut j: Bitlen, x: U, n: Bitlen) -> PcoResult<()> {
    Ok(())
  }

  pub fn write_usize_at(&mut self, bit_idx: usize, x: usize, n: Bitlen) -> PcoResult<()> {
    self.write_uint_at(bit_idx / 8, (bit_idx % 8) as Bitlen, x, n)
  }

  pub fn finish_byte(&mut self) {
    self.byte_idx += bits::ceil_div(self.bits_past_byte as usize, 8);
    self.bits_past_byte = 0;
  }
}

#[cfg(test)]
mod tests {
  use super::BitWriter;

  // I find little endian confusing, hence all the comments.
  // All the bytes are written backwards, e.g. 00000001 = 2^7

  #[test]
  fn test_write_bigger_num() {
    let mut writer = BitWriter::default();
    writer.write_diff(31_u32, 4);
    // 1111
    writer.write_usize(27, 4);
    // 11111101
    let bytes = writer.drain_bytes();
    assert_eq!(bytes, vec![191]);
  }

  #[test]
  fn test_long_diff_writes() {
    let mut writer = BitWriter::default();
    writer.write_usize((1 << 9) + (1 << 8) + 1, 9);
    // 10000000 1
    writer.write_usize((1 << 16) + (1 << 5) + 1, 17);
    // 10000000 11000010 00000000 01
    writer.write_usize(1 << 1, 17);
    // 10000000 11000010 00000000 01010000 00000000
    // 000
    writer.write_usize(1 << 1, 13);
    // 10000000 11000010 00000000 01010000 00000000
    // 00001000 00000000
    writer.write_usize((1 << 23) + (1 << 15), 24);
    // 10000000 11000010 00000000 01010000 00000000
    // 00001000 00000000 00000000 00000001 00000001

    let bytes = writer.drain_bytes();
    assert_eq!(
      bytes,
      // vec![128, 192, 8, 64, 0, 64, 2, 128, 128, 0],
      vec![1, 67, 0, 10, 0, 16, 0, 0, 128, 128],
    )
  }

  #[test]
  fn test_various_writes() {
    let mut writer = BitWriter::default();
    writer.write_one(true);
    writer.write_one(false);
    // 10
    writer.write_usize(33, 8);
    // 10100001 00
    writer.finish_byte();
    // 10100001 00000000
    writer.write_aligned_byte(123).expect("misaligned");
    // 10100001 00000000 11011110
    writer.write_diff(1964_u32, 12);
    // 10100001 00000000 11011110 00110101 1110
    writer.write_usize(5, 4);
    // 10100001 00000000 11011110 00110101 11101010
    writer.write_usize(5, 4);
    // 10100001 00000000 11011110 00110101 11101010
    // 10100000

    let bytes = writer.drain_bytes();
    assert_eq!(bytes, vec![133, 0, 123, 172, 87, 5],);
  }

  #[test]
  fn test_assign_usize() {
    let mut writer = BitWriter::default();
    writer.write_usize(0, 24);
    writer.write_usize_at(9, 129, 9);
    let bytes = writer.drain_bytes();
    assert_eq!(bytes, vec![0, 2, 1],);
  }
}
