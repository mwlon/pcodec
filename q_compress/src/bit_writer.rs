use crate::bits;
use crate::bits::BASE_BIT_MASK;
use crate::errors::{QCompressError, QCompressResult};
use crate::data_types::UnsignedLike;
use crate::constants::{BITS_TO_ENCODE_N_ENTRIES, BYTES_PER_WORD, MAX_ENTRIES, WORD_SIZE};

/// Builds compressed data, enabling a [`Compressor`][crate::Compressor] to
/// write bit-level information and ultimately output a `Vec<u8>`.
///
/// It does this by maintaining a bit index from 0 to `usize::BITS` within its
/// most recent `usize`.
///
/// The writer is consider is considered "aligned" if the current bit index
/// is byte-aligned; e.g. `bit_idx % 8 == 0`.
#[derive(Clone, Debug)]
pub struct BitWriter {
  word: usize,
  words: Vec<usize>,
  j: usize,
}

impl Default for BitWriter {
  fn default() -> Self {
    BitWriter {
      word: 0,
      words: Vec::new(),
      j: 0,
    }
  }
}

impl BitWriter {
  /// Returns the number of bytes so far produced by the writer.
  pub fn byte_size(&self) -> usize {
    self.words.len() * BYTES_PER_WORD + bits::ceil_div(self.j, 8)
  }

  /// Returns the number of bits so far produced by the writer.
  pub fn bit_size(&self) -> usize {
    self.words.len() * WORD_SIZE + self.j
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
        self.word |= (byte as usize) << (WORD_SIZE - 8 - self.j);
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
    if self.j == WORD_SIZE {
      self.words.push(self.word);
      self.word = 0;
      self.j = 0;
    }
  }

  /// Appends the bit to the writer.
  pub fn write_one(&mut self, b: bool) {
    self.refresh_if_needed();

    if b {
      self.word |= BASE_BIT_MASK >> self.j;
    }

    self.j += 1;
  }

  /// Appends the bits to the writer.
  pub fn write(&mut self, bs: &[bool]) {
    for &b in bs {
      self.write_one(b);
    }
  }

  pub fn write_usize(&mut self, x: usize, n: usize) {
    if n == 0 {
      return;
    }

    self.refresh_if_needed();

    let n_plus_j = n + self.j;
    if n_plus_j <= WORD_SIZE {
      let lshift = WORD_SIZE - n_plus_j;
      self.word |= (x << lshift) & (usize::MAX >> self.j);
      self.j = n_plus_j;
      return;
    }

    let remaining = n_plus_j - WORD_SIZE;
    self.words.push(self.word | ((x >> remaining) & (usize::MAX >> self.j)));

    // now remaining bits <= WORD_SIZE
    let lshift = WORD_SIZE - remaining;
    self.word = x << lshift;
    self.j = remaining;
  }

  pub fn write_diff<U: UnsignedLike>(&mut self, x: U, n: usize) {
    if n == 0 {
      return;
    }

    self.refresh_if_needed();

    let n_plus_j = n + self.j;
    if n_plus_j <= WORD_SIZE {
      let lshift = WORD_SIZE - n_plus_j;
      self.word |= x.lshift_word(lshift) & (usize::MAX >> self.j);
      self.j = n_plus_j;
      return;
    }

    let mut remaining = n_plus_j - WORD_SIZE;
    self.words.push(self.word | (x.rshift_word(remaining) & (usize::MAX >> self.j)));

    for _ in 0..(U::BITS - 1) / WORD_SIZE {
      if remaining <= WORD_SIZE {
        break;
      }

      let rshift = remaining - WORD_SIZE;
      self.words.push(x.rshift_word(rshift));
      remaining -= WORD_SIZE;
    }

    // now remaining bits <= WORD_SIZE
    let lshift = WORD_SIZE - remaining;
    self.word = x.lshift_word(lshift);
    self.j = remaining;
  }

  pub fn write_varint(&mut self, mut x: usize, jumpstart: usize) {
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
    self.j = bits::ceil_div(self.j, 8) * 8;
  }

  pub fn overwrite_usize(&mut self, bit_idx: usize, x: usize, n: usize) {
    // TODO
    let mut i = bit_idx / WORD_SIZE;
    let mut j = bit_idx % WORD_SIZE;
    // not the most efficient implementation but it's ok because we
    // only rarely use this now
    for k in 0..n {
      let b = (x >> (n - k - 1)) & 1 > 0;
      if j == WORD_SIZE {
        i += 1;
        j = 0;
      }
      let shift = WORD_SIZE - 1 - j;
      let mask = 1_usize << shift;
      let shifted_bit = (b as usize) << shift;
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

  #[test]
  fn test_write_bigger_num() {
    let mut writer = BitWriter::default();
    writer.write(&[true, true, true, true]);
    writer.write_usize(187, 4);
    let bytes = writer.drain_bytes();
    assert_eq!(
      bytes,
      vec![251],
    )
  }

  #[test]
  fn test_long_diff_writes() {
    let mut writer = BitWriter::default();
    // 10000000 11000000 00001000 01000000 00000000 01000000 00000010
    // 10000000 10000000 00000000
    writer.write_usize((1 << 9) + (1 << 8) + 1, 9);
    writer.write_usize((1 << 16) + (1 << 5) + 1, 17);
    writer.write_usize(1 << 1, 17);
    writer.write_usize(1 << 1, 13);
    writer.write_usize((1 << 23) + (1 << 15), 24);

    let bytes = writer.drain_bytes();
    assert_eq!(
      bytes,
      vec![128, 192, 8, 64, 0, 64, 2, 128, 128, 0],
    )
  }

  #[test]
  fn test_various_writes() {
    let mut writer = BitWriter::default();
    // 10001000 01000000 01111011 10010101 11100101 0101
    writer.write_one(true);
    writer.write_one(false);
    writer.write_usize(33, 8);
    writer.finish_byte();
    writer.write_aligned_byte(123).expect("misaligned");
    writer.write_varint(100, 3);
    writer.write_usize(5, 4);
    writer.write_usize(5, 4);

    println!("!! {:?} :: {}, {}", writer.words, writer.word, writer.j);
    let bytes = writer.drain_bytes();
    assert_eq!(
      bytes,
      vec![136, 64, 123, 149, 229, 80],
    );
  }

  #[test]
  fn test_assign_usize() {
    let mut writer = BitWriter::default();
    writer.write_usize(0, 24);
    writer.overwrite_usize(9, 129, 9);
    let bytes = writer.drain_bytes();
    assert_eq!(
      bytes,
      vec![0, 32, 64],
    );
  }
}
