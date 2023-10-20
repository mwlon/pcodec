use crate::ans::AnsState;
use std::cmp::min;
use std::fmt::{Debug, Display};
use std::ops::*;

use crate::bits;
use crate::constants::{Bitlen, BYTES_PER_WORD, WORD_BITLEN};
use crate::data_types::UnsignedLike;
use crate::errors::{PcoError, PcoResult};
use crate::read_write_uint::ReadWriteUint;

#[inline]
pub fn unchecked_word_at(src: &[u8], byte_idx: usize) -> usize {
  let raw_bytes = unsafe { *(src.as_ptr().add(byte_idx) as *const [u8; BYTES_PER_WORD]) };
  usize::from_le_bytes(raw_bytes)
}

#[inline]
pub fn unchecked_read_uint<U: ReadWriteUint, const MAX_EXTRA_WORDS: Bitlen>(
  src: &[u8],
  mut byte_idx: usize,
  bits_past_byte: Bitlen,
  n: Bitlen,
) -> U {
  let mut res = U::from_word(unchecked_word_at(src, byte_idx) >> bits_past_byte);
  let mut processed = min(n, WORD_BITLEN - 8 - bits_past_byte);
  byte_idx += BYTES_PER_WORD - 1;

  for _ in 0..MAX_EXTRA_WORDS {
    res |= U::from_word(unchecked_word_at(src, byte_idx)) << processed;
    processed = min(n, processed + WORD_BITLEN);
    byte_idx += BYTES_PER_WORD;
  }

  bits::lowest_bits(res, n)
}

struct Extension {
  data: Vec<u8>,
  padding: usize,
  skipped: usize,
}

pub struct BitReader<'a> {
  // immutable
  src: &'a [u8],
  // mutable
  extension: Option<Extension>,
  current_stream_skipped: usize,
  pub current_stream: &'a [u8], // either src or extension
  pub stale_byte_idx: usize, // in current stream
  pub bits_past_byte: Bitlen, // in current stream
}

impl<'a> From<&'a [u8]> for BitReader<'a> {
  fn from(src: &'a [u8]) -> Self {
    BitReader {
      src,
      extension: None,
      current_stream_skipped: 0,
      current_stream: src,
      stale_byte_idx: 0,
      bits_past_byte: 0
    }
  }
}

impl<'a> BitReader<'a> {
  pub fn current_stream_bit_idx(&self) -> usize {
    self.stale_byte_idx * 8 + self.bits_past_byte as usize
  }

  pub fn src_bit_idx(&self) -> usize {
    self.current_stream_skipped * 8 + self.current_stream_bit_idx()
  }

  fn switch_stream(&mut self, new_extension: Option<Extension>) {
    let old_skipped = self.current_stream_skipped;
    let new_skipped = new_extension.map_or(0, |ext| ext.skipped);
    self.stale_byte_idx = (self.stale_byte_idx + old_skipped) - new_skipped;
    self.current_stream_skipped = new_skipped;
    if new_extension.is_some() {
      self.current_stream = &new_extension.as_ref().unwrap().data;
      self.extension = new_extension;
    } else {
      self.current_stream = self.src;
    }
  }

  pub fn ensure_padded(&mut self, required_padding: usize) {
    let src_bit_idx = self.src_bit_idx();
    if bits::ceil_div(src_bit_idx, 8) + required_padding <= self.src.len() {
      self.switch_stream(None)
    } else {
      if self.extension.iter().all(|extension| extension.padding < required_padding) {
        // we need to create or grow the extension
        let skipped = src_bit_idx / 8;
        let copy_bytes = self.src.len() - skipped;
        let mut data = vec![0; copy_bytes + required_padding];
        data[..copy_bytes].copy_from_slice(&self.src[skipped..]);
        let extension = Some(Extension {
          data,
          padding: required_padding,
          skipped,
        });
        self.switch_stream(extension);
      }
    }
  }

  fn src_byte_idx(&self) -> usize {
    self.current_stream_skipped + self.stale_byte_idx + (self.bits_past_byte / 8) as usize
  }

  pub fn rest(&self) -> &'a [u8] {
    &self.src[self.src_byte_idx()..]
  }
// }
//
// #[derive(Clone)]
// pub struct BitReader<'a> {
//   // immutable
//   pub src: &'a [u8],
//   pub initial_bit_idx: usize,
//   // mutable
//   pub byte_idx: usize,
//   pub bits_past_byte: Bitlen,
// }

// impl<'a> From<&'a [u8]> for BitReader<'a> {
//   fn from(src: &'a [u8]) -> Self {
//     BitReader {
//       src,
//       byte_idx: 0,
//       bits_past_byte: 0,
//     }
//   }
// }
//
// impl<'a> BitReader<'a> {
//   pub fn new(src: &'a [u8], bit_idx: usize) -> Self {
//     BitReader {
//       src,
//       initial_bit_idx: bit_idx,
//       byte_idx: bit_idx / 8,
//       bits_past_byte: (bit_idx % 8) as Bitlen,
//     }
//   }

  // Returns the reader's current byte index. Will return an error if the
  // reader is at a misaligned position.
  pub fn aligned_byte_idx(&self) -> PcoResult<usize> {
    if self.bits_past_byte % 8 == 0 {
      Ok(self.stale_byte_idx + self.bits_past_byte as usize / 8)
    } else {
      Err(PcoError::invalid_argument(format!(
        "cannot get aligned byte index on misaligned bit reader (byte {} + {} bits)",
        self.stale_byte_idx, self.bits_past_byte,
      )))
    }
  }

  // fn insufficient_data_check(&self, name: &str, n: Bitlen) -> PcoResult<()> {
  //   let bit_idx = self.bit_idx();
  //   if bit_idx + n as usize > self.total_bits {
  //     Err(PcoError::insufficient_data_recipe(
  //       name,
  //       n,
  //       bit_idx,
  //       self.total_bits,
  //     ))
  //   } else {
  //     Ok(())
  //   }
  // }
  //
  #[inline]
  fn unchecked_word(&self) -> usize {
    unchecked_word_at(self.current_stream, self.stale_byte_idx)
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

  #[inline]
  fn consume_big(&mut self, n: usize) {
    let bit_idx = self.src_bit_idx() + n;
    self.seek_to(bit_idx);
  }

  pub fn read_aligned_bytes(&mut self, n: usize) -> PcoResult<&'a [u8]> {
    let byte_idx = self.aligned_byte_idx()?;
    let new_byte_idx = byte_idx + n;
    self.consume_big(n * 8);
    Ok(&self.current_stream[byte_idx..new_byte_idx])
  }

  pub fn read_usize(&mut self, n: Bitlen) -> usize {
    self.read_uint(n)
  }

  pub fn read_bitlen(&mut self, n: Bitlen) -> Bitlen {
    self.read_bitlen(n)
  }

  pub fn read_uint<U: ReadWriteUint>(&mut self, n: Bitlen) -> U {
    self.refill();
    let res = match U::MAX_EXTRA_WORDS {
      0 => unchecked_read_uint::<U, 0>(self.current_stream, self.stale_byte_idx, self.bits_past_byte, n),
      1 => unchecked_read_uint::<U, 1>(self.current_stream, self.stale_byte_idx, self.bits_past_byte, n),
      2 => unchecked_read_uint::<U, 2>(self.current_stream, self.stale_byte_idx, self.bits_past_byte, n),
      _ => panic!("data type is too large"),
    };
    self.consume(n);
    res
  }

  #[inline]
  pub fn read_small(&mut self, n: Bitlen) -> AnsState {
    self.refill();
    let res = unchecked_read_uint::<AnsState, 0>(self.current_stream, self.stale_byte_idx, self.bits_past_byte, n);
    self.consume(n);
    res
  }

  pub fn check_in_bounds(&self) -> PcoResult<()> {
    let src_bit_idx = self.src_bit_idx();
    let bit_len = self.src.len() * 8;
    if src_bit_idx > bit_len {
      return Err(PcoError::insufficient_data(format!(
        "reached bit idx {} / {}",
        src_bit_idx,
        bit_len,
      )));
    }
    Ok(())
  }

  // Seek to the end of the byte.
  // Used to skip to the next metadata or body section of the file, since they
  // always start byte-aligned.
  pub fn drain_empty_byte(&mut self, message: &str) -> PcoResult<()> {
    self.check_in_bounds()?;
    self.refill();
    if self.bits_past_byte != 0 {
      if (self.current_stream[self.stale_byte_idx] >> self.bits_past_byte) > 0 {
        return Err(PcoError::corruption(message));
      }
      self.consume(8 - self.bits_past_byte);
    }
    Ok(())
  }

  // Sets the bit reader's current position to the specified bit index.
  // Will NOT check whether the resulting position is in bounds or not.
  pub fn seek_to(&mut self, bit_idx: usize) {
    self.stale_byte_idx = bit_idx / 8 - self.current_stream_skipped;
    self.bits_past_byte = (bit_idx % 8) as Bitlen;
  }
}

// #[cfg(test)]
// mod tests {
//   use crate::bit_writer::BitWriter;
//   use crate::constants::WORD_BITLEN;
//   use crate::errors::PcoResult;
//
//   use super::BitReader;
//
//   #[test]
//   fn test_bit_reader() -> PcoResult<()> {
//     // bits: 1001 1010  1101 0110  1011 0100
//     let bytes = vec![0x9a, 0xd6, 0xb4];
//     let words = PaddedBytes::from(&bytes);
//     let mut bit_reader = BitReader::from(&words);
//     assert_eq!(bit_reader.read_aligned_bytes(1)?, vec![0x9a],);
//     assert_eq!(bit_reader.bit_idx(), 8);
//     bit_reader.seek_to(10);
//     assert_eq!(bit_reader.bit_idx(), 10);
//     bit_reader.read_uint::<u64>(3); // skip 3 bits
//     assert_eq!(bit_reader.bit_idx(), 13);
//     assert_eq!(
//       bit_reader.read_uint::<u64>(2),
//       2_u64
//     );
//     assert_eq!(bit_reader.bit_idx(), 15);
//     assert_eq!(bit_reader.read_small(3), 1_u32);
//     assert_eq!(bit_reader.bit_idx(), 18);
//     //leaves 1 bit left over
//     Ok(())
//   }
//
//   #[test]
//   fn test_writer_reader() -> PcoResult<()> {
//     let mut writer = BitWriter::default();
//     for i in 1..WORD_BITLEN + 1 {
//       writer.write_usize(i as usize, i)?;
//     }
//     let bytes = writer.drain_bytes();
//     let words = PaddedBytes::from(&bytes);
//     let mut usize_reader = BitReader::from(&words);
//     for i in 1..WORD_BITLEN + 1 {
//       assert_eq!(
//         usize_reader.read_uint::<usize>(i),
//         i as usize
//       );
//     }
//     Ok(())
//   }
// }
