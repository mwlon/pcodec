use std::cmp::min;
use std::fmt::{Debug, Display};
use std::ops::*;

use crate::bit_words::BitWords;
use crate::bits;
use crate::constants::{Bitlen, BITS_TO_ENCODE_N_ENTRIES, BYTES_PER_WORD, WORD_BITLEN};
use crate::data_types::UnsignedLike;
use crate::errors::{QCompressError, QCompressResult};

pub(crate) trait ReadableUint:
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

  fn from_word(word: usize) -> Self {
    word
  }
}

impl<U: UnsignedLike> ReadableUint for U {
  const ZERO: Self = <Self as UnsignedLike>::ZERO;
  const MAX: Self = <Self as UnsignedLike>::MAX;
  const BITS: Bitlen = <Self as UnsignedLike>::BITS;

  fn from_word(word: usize) -> Self {
    <Self as UnsignedLike>::from_word(word)
  }
}

/// Wrapper around compressed data, enabling a
/// [`Decompressor`][crate::Decompressor] to read
/// bit-level information and maintain its position in the data.
///
/// It does this with a slice of `usize`s representing the data and
/// maintaining
/// * an index into the slice and
/// * a bit index from 0 to `usize::BITS` within the current `usize`.
///
/// The reader is consider is considered "aligned" if the current bit index
/// is byte-aligned; e.g. `bit_idx % 8 == 0`.
#[derive(Clone)]
pub struct BitReader<'a> {
  // word = words[i], but must be carefully used and maintained:
  // * whenever i changes, we need to update word as well
  // * if we've reached the end of words, word will be 0, so be sure we're not exceeding bounds
  bytes: &'a [u8],
  // ptr: *const u8,
  bit_idx: usize,
  total_bits: usize,
}

impl<'a> From<&'a BitWords> for BitReader<'a> {
  fn from(bit_words: &'a BitWords) -> Self {
    BitReader {
      bytes: &bit_words.bytes,
      // ptr: bit_words.bytes.as_ptr(),
      bit_idx: 0,
      total_bits: bit_words.total_bits(),
    }
  }
}

impl<'a> BitReader<'a> {
  /// Returns the reader's current byte index. Will return an error if the
  /// reader is at
  /// a misaligned position.
  pub fn aligned_byte_idx(&self) -> QCompressResult<usize> {
    let (i, j) = self.idxs();
    if j == 0 {
      Ok(i)
    } else {
      Err(QCompressError::invalid_argument(format!(
        "cannot get aligned byte index on misaligned bit reader at bit {}",
        self.bit_idx
      )))
    }
  }

  pub fn bit_idx(&self) -> usize {
    self.bit_idx
  }

  /// Returns the number of bits between the reader's current position and
  /// the end.
  pub fn bits_remaining(&self) -> usize {
    self.total_bits - self.bit_idx()
  }

  /// Returns the number of bytes in the reader.
  pub fn byte_size(&self) -> usize {
    bits::ceil_div(self.total_bits, 8)
  }

  fn insufficient_data_check(&self, name: &str, n: Bitlen) -> QCompressResult<()> {
    let bit_idx = self.bit_idx();
    if bit_idx + n as usize > self.total_bits {
      Err(QCompressError::insufficient_data_recipe(
        name,
        n,
        bit_idx,
        self.total_bits,
      ))
    } else {
      Ok(())
    }
  }

  /// Returns the next `n` bytes. Will return an error if
  /// there are not enough bytes remaining in the reader or the reader is
  /// misaligned.
  pub fn read_aligned_bytes(&mut self, n: usize) -> QCompressResult<Vec<u8>> {
    let byte_idx = self.aligned_byte_idx()?;
    let new_byte_idx = byte_idx + n;
    let byte_size = self.byte_size();
    if new_byte_idx > byte_size {
      Err(QCompressError::insufficient_data(format!(
        "cannot read {} aligned bytes at byte idx {} out of {}",
        n, byte_idx, byte_size,
      )))
    } else {
      self.seek(n * 8);
      Ok(self.bytes[byte_idx..new_byte_idx].to_vec())
    }
  }

  /// Returns the next bit. Will return an error if we have reached the end
  /// of the reader.
  pub fn read_one(&mut self) -> QCompressResult<bool> {
    self.insufficient_data_check("read_one", 1)?;
    Ok(self.unchecked_read_one())
  }

  pub(crate) fn read_uint<U: ReadableUint>(&mut self, n: Bitlen) -> QCompressResult<U> {
    self.insufficient_data_check("read_uint", n)?;

    Ok(self.unchecked_read_uint::<U>(n))
  }

  pub fn read_usize(&mut self, n: Bitlen) -> QCompressResult<usize> {
    self.read_uint::<usize>(n)
  }

  pub fn read_bitlen(&mut self, n: Bitlen) -> QCompressResult<Bitlen> {
    self.read_uint::<Bitlen>(n)
  }

  fn idxs(&self) -> (usize, Bitlen) {
    (self.bit_idx >> 3, (self.bit_idx & 7) as Bitlen)
  }

  // returns (bits read, idx)
  pub fn read_bin_table_idx(&mut self, table_size_log: Bitlen) -> QCompressResult<(Bitlen, usize)> {
    let bit_idx = self.bit_idx();
    if bit_idx >= self.total_bits {
      return Err(QCompressError::insufficient_data_recipe(
        "read_bin_table_idx",
        1,
        bit_idx,
        self.total_bits,
      ));
    }

    let bits_read = min(self.total_bits - bit_idx, table_size_log as usize) as Bitlen;
    let res = self.read_usize(bits_read)?;
    Ok((bits_read, res))
  }

  pub fn read_varint(&mut self, jumpstart: Bitlen) -> QCompressResult<usize> {
    let mut res = self.read_usize(jumpstart)?;
    for i in jumpstart..BITS_TO_ENCODE_N_ENTRIES {
      if self.read_one()? {
        if self.read_one()? {
          res |= 1 << i
        }
      } else {
        break;
      }
    }
    Ok(res)
  }

  #[inline]
  fn unchecked_word(&self, i: usize) -> usize {
    // we can do this because BitWords made sure to pad self.bytes
    let raw_bytes = unsafe { *(self.bytes.as_ptr().add(i) as *const [u8; BYTES_PER_WORD]) };
    usize::from_le_bytes(raw_bytes)
  }

  /// Returns the next bit. Will panic if we have reached the end of the
  /// reader. This tends to be much faster than `read_one()`.
  pub fn unchecked_read_one(&mut self) -> bool {
    let (i, j) = self.idxs();
    let res = (self.bytes[i] & (1 << j)) > 0;
    self.bit_idx += 1;
    res
  }

  pub(crate) fn unchecked_read_uint<U: ReadableUint>(&mut self, n: Bitlen) -> U {
    if n == 0 {
      return U::ZERO;
    }

    let (mut i, j) = self.idxs();
    let mut res = U::from_word(self.unchecked_word(i) >> j);
    let mut processed = WORD_BITLEN - j;

    // This for loop looks redundant/slow, as if it could just be a while
    // loop, but its bounds get evaluated at compile time and it actually
    // speeds this up.
    for _ in 0..U::MAX_EXTRA_WORDS {
      if processed >= n {
        break;
      }
      i += BYTES_PER_WORD;
      res |= U::from_word(self.unchecked_word(i)) << processed;
      processed += WORD_BITLEN;
    }

    self.bit_idx += n as usize;
    res & (U::MAX >> (U::BITS - n))
  }

  pub fn unchecked_read_bin_table_idx(&mut self, table_size_log: Bitlen) -> usize {
    let (i, j) = self.idxs();
    self.bit_idx += table_size_log as usize;
    (self.unchecked_word(i) >> j) & (usize::MAX >> (WORD_BITLEN - table_size_log))
  }

  pub fn unchecked_read_varint(&mut self, jumpstart: Bitlen) -> usize {
    let mut res = self.unchecked_read_uint::<usize>(jumpstart);
    for i in jumpstart..BITS_TO_ENCODE_N_ENTRIES {
      if self.unchecked_read_one() {
        if self.unchecked_read_one() {
          res |= 1 << i
        }
      } else {
        break;
      }
    }
    res
  }

  // Seek to the end of the byte.
  // Used to skip to the next metadata or body section of the file, since they
  // always start byte-aligned.
  pub fn drain_empty_byte(&mut self, message: &str) -> QCompressResult<()> {
    let (i, j) = self.idxs();
    if j != 0 {
      if (self.bytes[i] >> j) > 0 {
        return Err(QCompressError::corruption(message));
      }
      let new_bit_idx = 8 * bits::ceil_div(self.bit_idx, 8);
      self.bit_idx = new_bit_idx;
    }
    Ok(())
  }

  /// Sets the bit reader's current position to the specified bit index.
  /// Will NOT check whether the resulting position is in bounds or not.
  pub fn seek_to(&mut self, bit_idx: usize) {
    self.bit_idx = bit_idx;
  }

  /// Skips forward `n` bits. Will NOT check whether
  /// the resulting position is in bounds or not.
  ///
  /// Wraps [`seek_to`][BitReader::seek_to].
  pub fn seek(&mut self, n: usize) {
    self.seek_to(self.bit_idx() + n);
  }

  /// Skips backward `n` bits where n <= 32.
  /// Will panic if the resulting position is out of bounds.
  pub fn rewind_bin_overshoot(&mut self, n: Bitlen) {
    self.bit_idx -= n as usize;
  }
}

#[cfg(test)]
mod tests {
  use super::BitReader;
  use crate::bit_words::BitWords;
  use crate::bit_writer::BitWriter;
  use crate::constants::{WORD_BITLEN};
  use crate::errors::QCompressResult;

  #[test]
  fn test_bit_reader() -> QCompressResult<()> {
    // bits: 1001 1010  1101 0110  1011 0100
    let bytes = vec![0x9a, 0xd6, 0xb4];
    let words = BitWords::from(&bytes);
    let mut bit_reader = BitReader::from(&words);
    assert_eq!(bit_reader.read_aligned_bytes(1)?, vec![0x9a],);
    assert!(!bit_reader.unchecked_read_one());
    assert!(bit_reader.read_one()?);
    bit_reader.seek(3);
    assert_eq!(
      bit_reader.unchecked_read_uint::<u64>(2),
      2_u64
    );
    assert_eq!(
      bit_reader.unchecked_read_uint::<u32>(3),
      1_u32
    );
    assert_eq!(bit_reader.unchecked_read_varint(2), 5);
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
    let words = BitWords::from(&bytes);
    let mut usize_reader = BitReader::from(&words);
    for i in 1..WORD_BITLEN + 1 {
      assert_eq!(
        usize_reader.unchecked_read_uint::<usize>(i),
        i as usize
      );
    }
  }

  #[test]
  fn test_read_bin_table_idx() -> QCompressResult<()> {
    let bytes = (0..17).collect::<Vec<_>>();
    let words = BitWords::from(bytes);
    let mut reader = BitReader::from(&words);
    reader.seek(56);
    // bytes 7 and 8, all data available
    assert_eq!(
      reader.read_bin_table_idx(16)?,
      (16, 7 + (8 << 8))
    );
    // byte 9 and part of 10
    assert_eq!(
      reader.read_bin_table_idx(11)?,
      (11, 9 + (2 << 8))
    );
    reader.seek_to(120);
    // 2 bytes left, but we'll ask for 3
    assert_eq!(
      reader.read_bin_table_idx(24)?,
      (16, 15 + (16 << 8))
    );
    Ok(())
  }

  #[test]
  fn test_seek_rewind() {
    let bytes = vec![0; 6];
    let words = BitWords::from(&bytes);
    let mut reader = BitReader::from(&words);
    reader.seek(43);

    reader.rewind_bin_overshoot(2);
    assert_eq!(reader.bit_idx(), 41);
    reader.rewind_bin_overshoot(2);
    assert_eq!(reader.bit_idx(), 39);
    reader.rewind_bin_overshoot(7);
    assert_eq!(reader.bit_idx(), 32);
    reader.rewind_bin_overshoot(8);
    assert_eq!(reader.bit_idx(), 24);
    reader.rewind_bin_overshoot(17);
    assert_eq!(reader.bit_idx(), 7);
  }
}
