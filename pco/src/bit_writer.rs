use std::cell::{Ref, RefCell, RefMut};
use std::cmp::{max, min};
use std::mem;

use crate::bits;
use crate::constants::{Bitlen, BYTES_PER_WORD, WORD_BITLEN, WORD_SIZE};
use crate::data_types::UnsignedLike;
use crate::errors::{PcoError, PcoResult};
use crate::read_write_uint::ReadWriteUint;
use crate::bit_reader::word_at;

#[inline]
pub fn write_word_to(word: usize, byte_idx: usize, dst: &mut [u8]) {
  unsafe {
    let target = (dst.as_mut_ptr().add(byte_idx) as *mut [u8; BYTES_PER_WORD]);
    *target = word.to_le_bytes();
  };
}

#[inline]
pub fn write_uint_to<U: ReadWriteUint, const MAX_EXTRA_WORDS: Bitlen>(
  x: U,
  mut byte_idx: usize,
  bits_past_byte: Bitlen,
  n: Bitlen,
  dst: &mut [u8],
) {
  let word = word_at(dst, byte_idx) | (x.to_usize() << bits_past_byte);
  write_word_to(word, byte_idx, dst);
  let mut processed = min(n, WORD_BITLEN - 8 - bits_past_byte);
  byte_idx += BYTES_PER_WORD - 1;

  for _ in 0..MAX_EXTRA_WORDS {
    write_word_to((x >> processed).to_usize(), byte_idx, dst);
    processed = min(n, processed + WORD_BITLEN);
    byte_idx += BYTES_PER_WORD;
  }
}

// Maybe I should rewrite this in a way that's generic to both BitReader and BitWriter
pub struct BitWriter<'a> {
  pub current_stream: &'a mut [u8], // either dst or extension
  other_stream: &'a mut [u8],
  current_is_dst: bool, // as opposed to extension
  padding: usize, // in extension
  skipped: usize, // in extension
  pub stale_byte_idx: usize, // in current stream
  pub bits_past_byte: Bitlen, // in current stream
}

impl<'a> BitWriter<'a> {
  pub fn new(dst: &'a mut [u8], extension: &'a mut [u8]) -> Self {
    // we assume extension has len min(dst.len(), padding) + padding
    // where the first min(dst.len(), padding) overlap with dst
    let padding = max(extension.len() / 2, extension.len().saturating_sub(dst.len()));
    let skipped = dst.len().saturating_sub(padding);
    Self {
      current_stream: dst,
      other_stream: extension,
      padding,
      skipped,
      stale_byte_idx: 0,
      bits_past_byte: 0,
      current_is_dst: true,
    }
  }

  fn byte_idx(&self) -> usize {
    self.stale_byte_idx + (self.bits_past_byte / 8) as usize
  }

  fn dst_bit_idx(&self) -> usize {
    let bit_idx = self.stale_byte_idx * 8 + self.bits_past_byte as usize;
    if self.current_is_dst {
      bit_idx
    } else {
      self.skipped * 8 + bit_idx
    }
  }

  fn dst_byte_idx(&self) -> usize {
    self.dst_bit_idx() / 8
  }

  fn switch_to_extension(&mut self) {
    assert!(self.current_is_dst);
    self.stale_byte_idx -= self.skipped;
    self.current_is_dst = false;
    mem::swap(&mut self.current_stream, &mut self.other_stream);
  }

  fn dst_byte_size(&self) -> usize {
    if self.current_is_dst {
      self.current_stream.len()
    } else {
      self.other_stream.len()
    }
  }

  fn dst_bit_size(&self) -> usize {
    self.dst_byte_size() * 8
  }

  pub fn check_in_bounds(&self) -> PcoResult<()> {
    let dst_bit_idx = self.dst_bit_idx();
    let dst_size = self.dst_bit_size();
    if dst_bit_idx > dst_size {
      return Err(PcoError::insufficient_data(format!(
        "out of bounds at bit {} / {}",
        dst_bit_idx,
        dst_size,
      )));
    }
    Ok(())
  }

  // TODO start using this
  pub fn ensure_padded(&mut self, required_padding: usize) -> PcoResult<()> {
    self.check_in_bounds()?;

    let byte_idx = self.byte_idx();
    if byte_idx + required_padding < self.current_stream.len() {
      return Ok(())
    }

    // see if we can switch to the other stream
    if self.current_is_dst && byte_idx + required_padding > self.other_stream.len() + self.padding {
      self.switch_to_extension();
      return Ok(())
    }

    Err(PcoError::insufficient_data(
      "insufficient padding; this is likely either a bug in pco or a result of\
      using too large a custom data type",
    ))
  }

  #[inline]
  fn refill(&mut self) {
    self.stale_byte_idx += (self.bits_past_byte / 8) as usize;
    self.bits_past_byte %= 8;
  }

  #[inline]
  fn consume(&mut self, n: Bitlen) {
    self.bits_past_byte += n;
  }

  pub fn write_aligned_bytes(&mut self, bytes: &[u8]) -> PcoResult<()> {
    if self.bits_past_byte % 8 == 0 {
      self.refill();

      let end = bytes.len() + self.stale_byte_idx;
      if end > self.current_stream.len() {
        return Err(PcoError::insufficient_data(format!(
          "cannot write {} more bytes with at byte {}/{}",
          bytes.len(),
          self.dst_byte_idx(),
          self.dst_byte_size(),
        )))
      }
      self.current_stream[self.stale_byte_idx..end].clone_from_slice(bytes);
      self.stale_byte_idx = end;

      Ok(())
    } else {
      Err(PcoError::invalid_argument(format!(
        "cannot write aligned bytes to unaligned writer (bit idx {})",
        self.bits_past_byte,
      )))
    }
  }

  pub fn write_uint<U: ReadWriteUint>(&mut self, x: U, n: Bitlen) {
    self.refill();
    match U::MAX_EXTRA_WORDS {
      0 => write_uint_to::<U, 0>(x, self.stale_byte_idx, self.bits_past_byte, n, self.current_stream),
      1 => write_uint_to::<U, 1>(x, self.stale_byte_idx, self.bits_past_byte, n, self.current_stream),
      2 => write_uint_to::<U, 2>(x, self.stale_byte_idx, self.bits_past_byte, n, self.current_stream),
      _ => panic!("[BitWriter] data type too large (extra words {} > 2)", U::MAX_EXTRA_WORDS),
    }
    self.consume(n);
  }

  pub fn write_usize(&mut self, x: usize, n: Bitlen) {
    self.write_uint(x, n)
  }

  pub fn write_bitlen(&mut self, x: Bitlen, n: Bitlen) {
    self.write_uint(x, n)
  }

  pub fn finish_byte(&mut self) {
    self.stale_byte_idx += bits::ceil_div(self.bits_past_byte as usize, 8);
    self.bits_past_byte = 0;
  }

  pub fn bytes_consumed(self) -> PcoResult<usize> {
    self.check_in_bounds()?;

    if self.bits_past_byte % 8 != 0 {
      panic!("dangling bits remain; this is likely a bug in pco");
    }

    let byte_idx = self.byte_idx();
    let res = if self.current_is_dst {
      byte_idx
    } else {
      byte_idx + self.skipped
    };
    Ok(res)
  }
}

impl<'a> Drop for BitWriter<'a> {
  fn drop(&mut self) {
    if self.current_is_dst {
      return;
    }

    for (dst_byte, extension_byte) in self.current_stream.iter_mut().zip(self.other_stream.iter_mut().skip(self.skipped)) {
      *dst_byte |= *extension_byte;
    }
  }
}

// #[cfg(test)]
// mod tests {
//   use super::BitWriter;
//
//   // I find little endian confusing, hence all the comments.
//   // All the bytes are written backwards, e.g. 00000001 = 2^7
//
//   #[test]
//   fn test_write_bigger_num() {
//     let mut writer = BitWriter::default();
//     writer.write_diff(31_u32, 4);
//     // 1111
//     writer.write_usize(27, 4);
//     // 11111101
//     let bytes = writer.drain_bytes();
//     assert_eq!(bytes, vec![191]);
//   }
//
//   #[test]
//   fn test_long_diff_writes() {
//     let mut writer = BitWriter::default();
//     writer.write_usize((1 << 9) + (1 << 8) + 1, 9);
//     // 10000000 1
//     writer.write_usize((1 << 16) + (1 << 5) + 1, 17);
//     // 10000000 11000010 00000000 01
//     writer.write_usize(1 << 1, 17);
//     // 10000000 11000010 00000000 01010000 00000000
//     // 000
//     writer.write_usize(1 << 1, 13);
//     // 10000000 11000010 00000000 01010000 00000000
//     // 00001000 00000000
//     writer.write_usize((1 << 23) + (1 << 15), 24);
//     // 10000000 11000010 00000000 01010000 00000000
//     // 00001000 00000000 00000000 00000001 00000001
//
//     let bytes = writer.drain_bytes();
//     assert_eq!(
//       bytes,
//       // vec![128, 192, 8, 64, 0, 64, 2, 128, 128, 0],
//       vec![1, 67, 0, 10, 0, 16, 0, 0, 128, 128],
//     )
//   }
//
//   #[test]
//   fn test_various_writes() {
//     let mut writer = BitWriter::default();
//     writer.write_one(true);
//     writer.write_one(false);
//     // 10
//     writer.write_usize(33, 8);
//     // 10100001 00
//     writer.finish_byte();
//     // 10100001 00000000
//     writer.write_aligned_byte(123).expect("misaligned");
//     // 10100001 00000000 11011110
//     writer.write_diff(1964_u32, 12);
//     // 10100001 00000000 11011110 00110101 1110
//     writer.write_usize(5, 4);
//     // 10100001 00000000 11011110 00110101 11101010
//     writer.write_usize(5, 4);
//     // 10100001 00000000 11011110 00110101 11101010
//     // 10100000
//
//     let bytes = writer.drain_bytes();
//     assert_eq!(bytes, vec![133, 0, 123, 172, 87, 5],);
//   }
//
//   #[test]
//   fn test_assign_usize() {
//     let mut writer = BitWriter::default();
//     writer.write_usize(0, 24);
//     writer.write_usize_at(9, 129, 9);
//     let bytes = writer.drain_bytes();
//     assert_eq!(bytes, vec![0, 2, 1],);
//   }
// }
