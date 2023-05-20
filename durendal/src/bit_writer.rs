use crate::bits;
use crate::constants::{Bitlen, BITS_TO_ENCODE_N_ENTRIES, BYTES_PER_WORD, MAX_ENTRIES, WORD_BITLEN, WORD_SIZE};
use crate::data_types::UnsignedLike;
use crate::errors::{QCompressError, QCompressResult};

/// Builds compressed data, enabling a [`Compressor`][crate::Compressor] to
/// write bit-level information and ultimately output a `Vec<u8>`.
///
/// It does this by maintaining a bit index from 0 to `usize::BITS` within its
/// most recent `usize`.
///
/// The writer is consider is considered "aligned" if the current bit index
/// is byte-aligned; e.g. `bit_idx % 8 == 0`.
#[derive(Clone, Debug, Default)]
pub struct BitWriter {
  word: usize,
  words: Vec<usize>,
  j: Bitlen,
}

impl BitWriter {
  /// Returns the number of bytes so far produced by the writer.
  pub fn byte_size(&self) -> usize {
    self.words.len() * BYTES_PER_WORD + bits::ceil_div(self.j as usize, 8)
  }

  /// Returns the number of bits so far produced by the writer.
  pub fn bit_size(&self) -> usize {
    self.words.len() * WORD_SIZE + self.j as usize
  }

  pub fn write_aligned_byte(&mut self, byte: u8) -> QCompressResult<()> {
    self.write_aligned_bytes(&[byte])
  }

  /// Appends the bits to the writer. Will return an error if the writer is
  /// misaligned.
  pub fn write_aligned_bytes(&mut self, bytes: &[u8]) -> QCompressResult<()> {
    if self.j % 8 == 0 {
      for &byte in bytes {
        self.refresh_if_needed();
        self.word |= (byte as usize) << self.j;
        self.j += 8;
      }
      Ok(())
    } else {
      Err(QCompressError::invalid_argument(format!(
        "cannot write aligned bytes to unaligned bit reader at word {} bit {}",
        self.words.len(),
        self.j,
      )))
    }
  }

  #[inline]
  fn refresh_if_needed(&mut self) {
    if self.j == WORD_BITLEN {
      self.words.push(self.word);
      self.word = 0;
      self.j = 0;
    }
  }

  /// Appends the bit to the writer.
  pub fn write_one(&mut self, b: bool) {
    self.refresh_if_needed();

    if b {
      self.word |= 1 << self.j;
    }

    self.j += 1;
  }

  /// Appends the bits to the writer.
  pub fn write(&mut self, bs: &[bool]) {
    for &b in bs {
      self.write_one(b);
    }
  }

  pub fn write_usize(&mut self, mut x: usize, n: Bitlen) {
    if n == 0 {
      return;
    }
    // mask out any more significant digits of x
    x &= usize::MAX >> (WORD_BITLEN - n);

    self.refresh_if_needed();

    self.word |= x << self.j;
    let n_plus_j = n + self.j;

    if n_plus_j <= WORD_BITLEN {
      self.j = n_plus_j;
      return;
    }

    self.words.push(self.word);
    let shift = WORD_BITLEN - self.j;
    self.word = x >> shift;
    self.j = n_plus_j - WORD_BITLEN;
  }

  pub fn write_bitlen(&mut self, x: Bitlen, n: Bitlen) {
    self.write_usize(x as usize, n);
  }

  pub fn write_diff<U: UnsignedLike>(&mut self, mut x: U, n: Bitlen) {
    if n == 0 {
      return;
    }

    // mask out any more significant digits of x
    x &= U::MAX >> (U::BITS - n);

    self.refresh_if_needed();

    self.word |= x.lshift_word(self.j);
    let n_plus_j = n + self.j;
    if n_plus_j <= WORD_BITLEN {
      self.j = n_plus_j;
      return;
    }

    let mut processed = WORD_BITLEN - self.j;
    self.words.push(self.word);

    for _ in 0..(U::BITS - 1) / WORD_BITLEN {
      if n <= processed + WORD_BITLEN {
        break;
      }

      self.words.push(x.rshift_word(processed));
      processed += WORD_BITLEN;
    }

    // now remaining bits <= WORD_SIZE
    self.word = x.rshift_word(processed);
    self.j = n - processed;
  }

  pub fn write_varint(&mut self, mut x: usize, jumpstart: Bitlen) {
    if x > MAX_ENTRIES {
      panic!("unable to encode varint greater than max number of entries");
    }

    self.write_usize(x, jumpstart);
    x >>= jumpstart;
    for _ in jumpstart..BITS_TO_ENCODE_N_ENTRIES {
      if x > 0 {
        self.write_one(true);
        self.write_one(x & 1 > 0);
        x >>= 1;
      } else {
        break;
      }
    }
    self.write_one(false);
  }

  pub fn finish_byte(&mut self) {
    self.j = bits::ceil_div(self.j as usize, 8) as Bitlen * 8;
  }

  pub fn overwrite_usize(&mut self, bit_idx: usize, x: usize, n: Bitlen) {
    let mut i = bit_idx / WORD_SIZE;
    let mut j = bit_idx % WORD_SIZE;
    // not the most efficient implementation but it's ok because we
    // only rarely use this now
    for k in 0..n {
      let b = (x >> k) & 1 > 0;
      if j == WORD_SIZE {
        i += 1;
        j = 0;
      }
      let mask = 1_usize << j;
      let shifted_bit = (b as usize) << j;
      let word = self.words.get_mut(i).unwrap_or(&mut self.word);
      if *word & mask != shifted_bit {
        *word ^= shifted_bit;
      }
      j += 1;
    }
  }

  pub fn drain_bytes(&mut self) -> Vec<u8> {
    let byte_size = self.byte_size();
    self.words.push(self.word);
    self.word = 0;
    let mut res = bits::words_to_bytes(&self.words);
    res.truncate(byte_size);

    self.words.clear();
    self.j = 0;

    res
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
    writer.write(&[true, true, true, true]);
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
    writer.write_varint(100, 3);
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
    writer.overwrite_usize(9, 129, 9);
    let bytes = writer.drain_bytes();
    assert_eq!(bytes, vec![0, 2, 1],);
  }
}
