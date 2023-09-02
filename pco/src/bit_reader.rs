use std::cmp::min;
use std::fmt::{Debug, Display};
use std::ops::*;

use crate::bit_words::PaddedBytes;
use crate::bits;
use crate::constants::{Bitlen, BYTES_PER_WORD, WORD_BITLEN};
use crate::data_types::UnsignedLike;
use crate::errors::{PcoError, PcoResult};

pub trait ReadableUint:
  Add<Output = Self>
  + BitAnd<Output = Self>
  + BitOr<Output = Self>
  + BitAndAssign
  + BitOrAssign
  + Copy
  + Debug
  + Display
  + Shl<Bitlen, Output = Self>
  + Shr<Bitlen, Output = Self>
{
  const ZERO: Self;
  const MAX: Self;
  const BITS: Bitlen;
  const MAX_EXTRA_WORDS: Bitlen = (Self::BITS + 6) / WORD_BITLEN;

  fn from_word(word: usize) -> Self;
}

impl ReadableUint for usize {
  const ZERO: Self = 0;
  const MAX: Self = usize::MAX;
  const BITS: Bitlen = WORD_BITLEN;

  #[inline]
  fn from_word(word: usize) -> Self {
    word
  }
}

impl<U: UnsignedLike> ReadableUint for U {
  const ZERO: Self = <Self as UnsignedLike>::ZERO;
  const MAX: Self = <Self as UnsignedLike>::MAX;
  const BITS: Bitlen = <Self as UnsignedLike>::BITS;

  #[inline]
  fn from_word(word: usize) -> Self {
    <Self as UnsignedLike>::from_word(word)
  }
}

// Wrapper around compressed data, enabling a
// [`Decompressor`][crate::Decompressor] to read
// bit-level information and maintain its position in the data.
//
// It does this with a slice of `usize`s representing the data and
// maintaining
// * an index into the slice and
// * a bit index from 0 to `usize::BITS` within the current `usize`.
//
// The reader is consider is considered "aligned" if the current bit index
// is byte-aligned; e.g. `bit_idx % 8 == 0`.
#[derive(Clone)]
pub struct BitReader<'a> {
  // immutable
  bytes: &'a [u8],
  total_bits: usize,
  // mutable
  pub loaded_byte_idx: usize,
  pub bits_past_ptr: Bitlen,
  // buffer: usize,
}

impl<'a> From<&'a PaddedBytes> for BitReader<'a> {
  fn from(bit_words: &'a PaddedBytes) -> Self {
    BitReader {
      bytes: &bit_words.bytes,
      total_bits: bit_words.total_bits(),
      loaded_byte_idx: 0,
      bits_past_ptr: 0,
      // buffer: 0,
    }
  }
}

impl<'a> BitReader<'a> {
  // Returns the reader's current byte index. Will return an error if the
  // reader is at a misaligned position.
  pub fn aligned_byte_idx(&self) -> PcoResult<usize> {
    if self.bits_past_ptr % 8 == 0 {
      Ok(self.loaded_byte_idx + (self.bits_past_ptr / 8) as usize)
    } else {
      Err(PcoError::invalid_argument(format!(
        "cannot get aligned byte index on misaligned bit reader (byte {} + {} bits)",
        self.loaded_byte_idx,
        self.bits_past_ptr,
      )))
    }
  }

  pub fn bit_idx(&self) -> usize {
    self.loaded_byte_idx * 8 + self.bits_past_ptr as usize
  }

  // Returns the number of bits between the reader's current position and
  // the end.
  pub fn bits_remaining(&self) -> usize {
    self.total_bits - self.bit_idx()
  }

  // Returns the number of bytes in the reader.
  pub fn byte_size(&self) -> usize {
    bits::ceil_div(self.total_bits, 8)
  }

  fn insufficient_data_check(&self, name: &str, n: Bitlen) -> PcoResult<()> {
    let bit_idx = self.bit_idx();
    if bit_idx + n as usize > self.total_bits {
      Err(PcoError::insufficient_data_recipe(
        name,
        n,
        bit_idx,
        self.total_bits,
      ))
    } else {
      Ok(())
    }
  }

  #[inline]
  pub fn unchecked_word_at(&self, byte_idx: usize) -> usize {
    // we can do this because BitWords made sure to pad self.bytes
    let raw_bytes = unsafe { *(self.bytes.as_ptr().add(byte_idx) as *const [u8; BYTES_PER_WORD]) };
    usize::from_le_bytes(raw_bytes)
  }

  #[inline]
  fn unchecked_word(&self) -> usize {
    self.unchecked_word_at(self.loaded_byte_idx)
  }

  #[inline]
  fn refill(&mut self) {
    self.loaded_byte_idx += (self.bits_past_ptr / 8) as usize;
    // self.buffer = self.unchecked_word_at(self.loaded_byte_idx);
    self.bits_past_ptr = self.bits_past_ptr % 8;
  }

  #[inline]
  fn consume(&mut self, n: Bitlen) {
    self.bits_past_ptr += n;
  }

  #[inline]
  fn consume_big(&mut self, n: usize) {
    let bit_idx = self.bit_idx() + n;
    self.seek_to(bit_idx);
  }

  // Returns the next `n` bytes. Will return an error if
  // there are not enough bytes remaining in the reader or the reader is
  // misaligned.
  pub fn read_aligned_bytes(&mut self, n: usize) -> PcoResult<&'a [u8]> {
    let byte_idx = self.aligned_byte_idx()?;
    let new_byte_idx = byte_idx + n;
    let byte_size = self.byte_size();
    if new_byte_idx > byte_size {
      Err(PcoError::insufficient_data(format!(
        "cannot read {} aligned bytes at byte idx {} out of {}",
        n, byte_idx, byte_size,
      )))
    } else {
      self.consume_big(n * 8);
      Ok(&self.bytes[byte_idx..new_byte_idx])
    }
  }

  // Returns the next bit. Will return an error if we have reached the end
  // of the reader.
  pub fn read_one(&mut self) -> PcoResult<bool> {
    self.insufficient_data_check("read_one", 1)?;
    Ok(self.unchecked_read_one())
  }

  pub fn read_uint<U: ReadableUint>(&mut self, n: Bitlen) -> PcoResult<U> {
    self.insufficient_data_check("read_uint", n)?;
    Ok(self.unchecked_read_uint::<U>(n))
  }

  pub fn peek_uint<U: ReadableUint>(&self, bit_idx: usize, n: Bitlen) -> PcoResult<U> {
    self.insufficient_data_check("peek_uint", n)?;
    Ok(self.unchecked_peek_uint::<U>(bit_idx, n))
  }

  pub fn read_usize(&mut self, n: Bitlen) -> PcoResult<usize> {
    self.read_uint::<usize>(n)
  }

  pub fn read_bitlen(&mut self, n: Bitlen) -> PcoResult<Bitlen> {
    self.read_uint::<Bitlen>(n)
  }

  pub fn read_small(&mut self, n: Bitlen) -> PcoResult<usize> {
    self.insufficient_data_check("read_small", n)?;
    Ok(self.unchecked_read_small(n))
  }

  // Returns the next bit. Will panic if we have reached the end of the
  // reader. This tends to be much faster than `read_one()`.
  pub fn unchecked_read_one(&mut self) -> bool {
    self.refill();
    let res = self.bytes[self.loaded_byte_idx] & (1 << self.bits_past_ptr) > 0;
    self.consume(1);
    res
  }

  pub fn unchecked_read_uint<U: ReadableUint>(&mut self, n: Bitlen) -> U {
    if n == 0 {
      return U::ZERO;
    }

    self.refill();

    let mut res = U::from_word(self.unchecked_word() >> self.bits_past_ptr);
    let mut processed = WORD_BITLEN - self.bits_past_ptr;
    self.consume(min(processed, n));

    // This for loop looks redundant/slow, as if it could just be a while
    // loop, but its bounds get evaluated at compile time and it actually
    // speeds this up.
    for _ in 0..U::MAX_EXTRA_WORDS {
      if processed >= n {
        break;
      }
      self.refill();
      res |= U::from_word(self.unchecked_word()) << processed;
      self.consume(min(WORD_BITLEN, n - processed));
      processed += WORD_BITLEN;
    }

    res & (U::MAX >> (U::BITS - n))
  }

  pub fn unchecked_peek_uint<U: ReadableUint>(&self, pos: usize, n: Bitlen) -> U {
    if n == 0 {
      return U::ZERO;
    }

    let mut i = pos / 8;
    let j = (pos as Bitlen) % 8;

    let mut res = U::from_word(self.unchecked_word_at(i) >> j);
    let mut processed = WORD_BITLEN - j;
    i += if j == 0 { BYTES_PER_WORD } else { BYTES_PER_WORD - 1};

    // This for loop looks redundant/slow, as if it could just be a while
    // loop, but its bounds get evaluated at compile time and it actually
    // speeds this up.
    for _ in 0..U::MAX_EXTRA_WORDS {
      if processed >= n {
        break;
      }
      res |= U::from_word(self.unchecked_word_at(i)) << processed;
      processed += WORD_BITLEN;
      i += BYTES_PER_WORD;
    }

    res & (U::MAX >> (U::BITS - n))
  }

  #[inline]
  pub fn unchecked_read_small(&mut self, n: Bitlen) -> usize {
    self.refill();
    let unmasked = <usize as ReadableUint>::from_word(self.unchecked_word() >> self.bits_past_ptr);
    self.consume(n);
    // unmasked & ((1 << n) - 1)
    unmasked & (usize::MAX >> (WORD_BITLEN - n))
  }

  // Seek to the end of the byte.
  // Used to skip to the next metadata or body section of the file, since they
  // always start byte-aligned.
  pub fn drain_empty_byte(&mut self, message: &str) -> PcoResult<()> {
    self.refill();
    if self.bits_past_ptr != 0 {
      if (self.bytes[self.loaded_byte_idx] >> self.bits_past_ptr) > 0 {
        return Err(PcoError::corruption(message));
      }
      self.consume(8 - self.bits_past_ptr);
    }
    Ok(())
  }

  // Sets the bit reader's current position to the specified bit index.
  // Will NOT check whether the resulting position is in bounds or not.
  pub fn seek_to(&mut self, bit_idx: usize) {
    self.loaded_byte_idx = bit_idx / 8;
    self.bits_past_ptr = (bit_idx % 8) as Bitlen;
    self.refill();
  }
}

#[cfg(test)]
mod tests {
  use crate::bit_words::PaddedBytes;
  use crate::bit_writer::BitWriter;
  use crate::constants::WORD_BITLEN;
  use crate::errors::PcoResult;

  use super::BitReader;

  #[test]
  fn test_bit_reader() -> PcoResult<()> {
    // bits: 1001 1010  1101 0110  1011 0100
    let bytes = vec![0x9a, 0xd6, 0xb4];
    let words = PaddedBytes::from(&bytes);
    let mut bit_reader = BitReader::from(&words);
    assert_eq!(bit_reader.read_aligned_bytes(1)?, vec![0x9a],);
    assert_eq!(bit_reader.bit_idx(), 8);
    assert!(!bit_reader.unchecked_read_one());
    assert_eq!(bit_reader.bit_idx(), 9);
    assert!(bit_reader.read_one()?);
    assert_eq!(bit_reader.bit_idx(), 10);
    bit_reader.unchecked_read_uint::<u64>(3); // skip 3 bits
    assert_eq!(bit_reader.bit_idx(), 13);
    assert_eq!(
      bit_reader.unchecked_read_uint::<u64>(2),
      2_u64
    );
    assert_eq!(bit_reader.bit_idx(), 15);
    assert_eq!(bit_reader.unchecked_read_small(3), 1_u32);
    assert_eq!(bit_reader.bit_idx(), 18);
    //leaves 1 bit left over
    Ok(())
  }

  #[test]
  fn test_writer_reader() {
    let mut writer = BitWriter::default();
    for i in 1..WORD_BITLEN + 1 {
      writer.write_usize(i as usize, i);
    }
    let bytes = writer.drain_bytes();
    let words = PaddedBytes::from(&bytes);
    let mut usize_reader = BitReader::from(&words);
    for i in 1..WORD_BITLEN + 1 {
      assert_eq!(
        usize_reader.unchecked_read_uint::<usize>(i),
        i as usize
      );
    }
  }
}
