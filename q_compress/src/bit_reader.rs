use std::cmp::min;
use crate::bit_words::BitWords;

use crate::bits;
use crate::constants::{BITS_TO_ENCODE_N_ENTRIES, BYTES_PER_WORD, WORD_SIZE};
use crate::data_types::UnsignedLike;
use crate::errors::{QCompressError, QCompressResult};

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
  words: &'a [usize],
  i: usize,
  j: usize,
  total_bits: usize,
}

impl<'a> From<&'a BitWords> for BitReader<'a> {
  fn from(bit_words: &'a BitWords) -> Self {
    BitReader {
      words: &bit_words.words,
      i: 0,
      j: 0,
      total_bits: bit_words.total_bits,
    }
  }
}

impl<'a> BitReader<'a> {
  /// Returns the reader's current byte index. Will return an error if the
  /// reader is at
  /// a misaligned position.
  pub fn aligned_byte_idx(&self) -> QCompressResult<usize> {
    if self.j % 8 == 0 {
      Ok(self.i * BYTES_PER_WORD + self.j / 8)
    } else {
      Err(QCompressError::invalid_argument(format!(
        "cannot get aligned byte index on misaligned bit reader at word {} bit {}",
        self.i,
        self.j,
      )))
    }
  }
  
  pub(crate) fn bit_idx(&self) -> usize {
    WORD_SIZE * self.i + self.j
  }

  /// Returns the number of bits between the reader's current position and
  /// the end.
  pub fn bits_remaining(&self) -> usize {
    self.total_bits - self.bit_idx()
  }

  /// Returns the number of bytes in the reader.
  pub fn byte_size(&self) -> usize {
    self.total_bits / 8
  }

  fn refresh_if_needed(&mut self) {
    if self.j == WORD_SIZE {
      self.i += 1;
      self.j = 0;
    }
  }

  fn insufficient_data_check(&self, name: &str, n: usize) -> QCompressResult<()> {
    let bit_idx = self.bit_idx();
    if bit_idx + n > self.total_bits {
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
    self.refresh_if_needed();

    let byte_idx = self.aligned_byte_idx()?;
    let new_byte_idx = byte_idx + n;
    let byte_size = self.byte_size();
    if new_byte_idx > byte_size {
      Err(QCompressError::insufficient_data(format!(
        "cannot read {} aligned bytes at byte idx {} out of {}",
        n,
        byte_idx,
        byte_size,
      )))
    } else {
      let end_word_idx = bits::ceil_div(new_byte_idx, BYTES_PER_WORD);
      let padded_bytes = bits::words_to_bytes(&self.words[byte_idx / BYTES_PER_WORD..end_word_idx]);

      self.seek(n * 8);
      let padded_start_idx = byte_idx % BYTES_PER_WORD;
      Ok(padded_bytes[padded_start_idx..padded_start_idx + n].to_vec())
    }
  }

  /// Returns the next bit. Will return an error if we have reached the end
  /// of the reader.
  pub fn read_one(&mut self) -> QCompressResult<bool> {
    self.insufficient_data_check("read_one", 1)?;
    self.refresh_if_needed();

    let res = bits::bit_from_word(self.unchecked_word(), self.j);
    self.j += 1;
    Ok(res)
  }

  /// Returns the next `n` bits. Will return an error if there are not
  /// enough bits remaining.
  pub fn read(&mut self, n: usize) -> QCompressResult<Vec<bool>> {
    self.insufficient_data_check("read", n)?;

    let mut res = Vec::with_capacity(n);

    // implementation not well optimized because this is only used in reading header
    let mut word = self.unchecked_word();
    for _ in 0..n {
      if self.j == WORD_SIZE {
        self.i += 1;
        self.j = 0;
        word = self.unchecked_word();
      }
      res.push(bits::bit_from_word(word, self.j));
      self.j += 1;
    }
    Ok(res)
  }

  pub(crate) fn read_diff<Diff: UnsignedLike>(&mut self, n: usize) -> QCompressResult<Diff> {
    self.insufficient_data_check("read_diff", n)?;

    Ok(self.unchecked_read_diff::<Diff>(n))
  }

  pub(crate) fn read_usize(&mut self, n: usize) -> QCompressResult<usize> {
    Ok(self.read_diff::<u64>(n)? as usize)
  }

  // returns (bits read, idx)
  pub(crate) fn read_prefix_table_idx(
    &mut self,
    table_size_log: usize,
  ) -> QCompressResult<(usize, usize)> {
    let bit_idx = self.bit_idx();
    if bit_idx >= self.total_bits {
      return Err(QCompressError::insufficient_data_recipe(
        "read_prefix_table_idx",
        1,
        bit_idx,
        self.total_bits,
      ));
    }

    self.refresh_if_needed();

    let n_plus_j = table_size_log + self.j;
    if n_plus_j <= WORD_SIZE {
      let rshift = WORD_SIZE - n_plus_j;
      let res = (self.unchecked_word() & (usize::MAX >> self.j)) >> rshift;
      let bits_read = min(table_size_log, self.total_bits - bit_idx);
      self.j += bits_read;
      Ok((bits_read, res))
    } else {
      let remaining = n_plus_j - WORD_SIZE;
      let mut res = (self.unchecked_word() & (usize::MAX >> self.j)) << remaining;
      self.i += 1;
      if self.i < self.words.len() {
        let shift = WORD_SIZE - remaining;
        res |= self.unchecked_word() >> shift;
        self.j = remaining;
        Ok((table_size_log, res))
      } else {
        self.j = WORD_SIZE;
        Ok((table_size_log - remaining, res))
      }
    }
  }

  pub(crate) fn read_varint(&mut self, jumpstart: usize) -> QCompressResult<usize> {
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

  fn unchecked_word(&self) -> usize {
    self.words[self.i]
  }

  /// Returns the next bit. Will panic if we have reached the end of the
  /// reader. This tends to be much faster than `read_one()`.
  pub fn unchecked_read_one(&mut self) -> bool {
    self.refresh_if_needed();

    let res = bits::bit_from_word(self.unchecked_word(), self.j);
    self.j += 1;
    res
  }

  pub(crate) fn unchecked_read_diff<Diff: UnsignedLike>(&mut self, n: usize) -> Diff {
    if n == 0 {
      return Diff::ZERO;
    }

    self.refresh_if_needed();

    let n_plus_j = n + self.j;
    if n_plus_j <= WORD_SIZE {
      // it's all in the current word
      let shift = WORD_SIZE - n_plus_j;
      let res = Diff::from_word((self.unchecked_word() & (usize::MAX >> self.j)) >> shift);
      self.j = n_plus_j;
      res
    } else {
      let mut res = Diff::ZERO;
      let mut remaining = n_plus_j - WORD_SIZE;
      res |= Diff::from_word(self.unchecked_word() & (usize::MAX >> self.j)) << remaining;
      while remaining >= WORD_SIZE {
        self.i += 1;
        remaining -= WORD_SIZE;
        res |= Diff::from_word(self.unchecked_word()) << remaining;
      }
      if remaining > 0 {
        self.i += 1;
        let shift = WORD_SIZE - remaining;
        res |= Diff::from_word(self.unchecked_word() >> shift);
        self.j = remaining;
      } else {
        self.j = WORD_SIZE;
      }
      res
    }
  }

  pub(crate) fn unchecked_read_prefix_table_idx(
    &mut self,
    table_size_log: usize,
  ) -> usize {
    self.refresh_if_needed();

    let n_plus_j = table_size_log + self.j;
    if n_plus_j <= WORD_SIZE {
      let shift = WORD_SIZE - n_plus_j;
      let res = (self.unchecked_word() & (usize::MAX >> self.j)) >> shift;
      self.j = n_plus_j;
      res
    } else {
      let remaining = n_plus_j - WORD_SIZE;
      let mut res = ((self.unchecked_word() & (usize::MAX >> self.j)) as usize) << remaining;
      self.i += 1;
      let shift = WORD_SIZE - remaining;
      res |= self.unchecked_word() >> shift;
      self.j = remaining;
      res
    }
  }

  pub(crate) fn unchecked_read_varint(&mut self, jumpstart: usize) -> usize {
    let mut res = self.unchecked_read_diff::<u64>(jumpstart) as usize;
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
  pub(crate) fn drain_empty_byte<F>(&mut self, f: F) -> QCompressResult<()>
  where F: FnOnce() -> QCompressError {
    if self.j % 8 != 0 {
      let end_j = 8 * bits::ceil_div(self.j, 8);
      if self.unchecked_word() & (usize::MAX >> self.j) & (usize::MAX << (WORD_SIZE - end_j)) > 0 {
        return Err(f());
      }
      self.j = end_j;
    }
    Ok(())
  }

  /// Skips forward `n` bits. Will NOT check whether
  /// the resulting position is in bounds or not.
  pub fn seek(&mut self, n: usize) {
    let forward_bit_idx = self.j + n;
    self.i += forward_bit_idx.div_euclid(WORD_SIZE);
    self.j = forward_bit_idx.rem_euclid(WORD_SIZE);
  }

  /// Skips backward `n` bits. Will panic if the resulting position is out of
  /// bounds.
  pub fn rewind(&mut self, n: usize) {
    if n <= self.j {
      self.j -= n;
    } else {
      let backward_bit_idx = (n + WORD_SIZE - 1) - self.j;
      self.i -= backward_bit_idx.div_euclid(WORD_SIZE);
      self.j = WORD_SIZE - 1 - backward_bit_idx.rem_euclid(WORD_SIZE);
    }
  }
}

#[cfg(test)]
mod tests {
  use crate::bit_words::BitWords;
  use crate::BitReader;
  use crate::errors::QCompressResult;

  #[test]
  fn test_bit_reader() -> QCompressResult<()>{
    // bits: 1001 1010  0110 1011  0010 1101
    let bytes = vec![0x9a, 0x6b, 0x2d];
    let words = BitWords::from(&bytes);
    let mut bit_reader = BitReader::from(&words);
    assert_eq!(
      bit_reader.read_aligned_bytes(1)?,
      vec![0x9a],
    );
    assert!(!bit_reader.unchecked_read_one());
    assert!(bit_reader.read_one()?);
    assert_eq!(
      bit_reader.read(3)?,
      vec![true, false, true],
    );
    assert_eq!(
      bit_reader.unchecked_read_diff::<u64>(2),
      1_u64
    );
    assert_eq!(
      bit_reader.unchecked_read_diff::<u32>(3),
      4_u32
    );
    assert_eq!(
      bit_reader.unchecked_read_varint(2),
      6
    );
    //leaves 1 bit left over
    Ok(())
  }

  #[test]
  fn test_seek_rewind() {
    let bytes = vec![0; 6];
    let words = BitWords::from(&bytes);
    let mut reader = BitReader::from(&words);
    reader.seek(43);

    reader.rewind(2);
    assert_eq!(reader.bit_idx(), 41);
    reader.rewind(2);
    assert_eq!(reader.bit_idx(), 39);
    reader.rewind(7);
    assert_eq!(reader.bit_idx(), 32);
    reader.rewind(8);
    assert_eq!(reader.bit_idx(), 24);
    reader.rewind(17);
    assert_eq!(reader.bit_idx(), 7);
  }
}
