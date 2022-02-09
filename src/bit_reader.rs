use std::fmt::{Debug, Formatter};
use std::fmt;

use crate::bits;
use crate::bits::{LEFT_MASKS, RIGHT_MASKS};
use crate::constants::{PREFIX_TABLE_SIZE_LOG, BITS_TO_ENCODE_N_ENTRIES};
use crate::errors::{QCompressError, QCompressResult};
use crate::data_types::UnsignedLike;

/// `BitReader` wraps bytes during decompression, enabling a decompressor
/// to read bit-level information and maintain its position in the bytes.
///
/// It does this by maintaining
/// * a byte index and
/// * a bit index from 0-8 within that byte.
///
/// The reader is consider is considered "aligned" if the current bit index
/// is 0 or 8 (i.e. at the start or end of the current byte).
#[derive(Clone)]
pub struct BitReader {
  bytes: Vec<u8>,
  i: usize,
  j: usize,
  total_bits: usize,
}

impl Debug for BitReader {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    let current_info = if self.i < self.bytes.len() {
      format!(
        "current byte {}",
        self.bytes[self.i],
      )
    } else {
      "OOB".to_string()
    };

    write!(
      f,
      "BitReader(\n\tbyte {}/{} bit {}\n\t{}\n\t)",
      self.i,
      self.bytes.len(),
      self.j,
      current_info,
    )
  }
}

impl From<Vec<u8>> for BitReader {
  fn from(bytes: Vec<u8>) -> BitReader {
    let total_bits = 8 * bytes.len();
    BitReader {
      bytes,
      i: 0,
      j: 0,
      total_bits,
    }
  }
}

impl BitReader {
  /// Returns the reader's current byte index. Will return an error if the
  /// reader is at
  /// a misaligned position.
  pub fn aligned_byte_ind(&self) -> QCompressResult<usize> {
    if self.j == 0 {
      Ok(self.i)
    } else if self.j == 8 {
      Ok(self.i + 1)
    } else {
      Err(QCompressError::invalid_argument(format!(
        "cannot get aligned byte index on misaligned bit reader at byte {} bit {}",
        self.i,
        self.j,
      )))
    }
  }

  /// Returns the number of bits between the reader's current position and
  /// the end.
  pub fn bits_remaining(&self) -> usize {
    self.total_bits - 8 * self.i - self.j
  }

  /// Returns the number of bytes in the reader.
  pub fn byte_size(&self) -> usize {
    self.bytes.len()
  }

  /// Returns the reader's current (byte_idx, bit_idx) tuple.
  pub fn inds(&self) -> (usize, usize) {
    (self.i, self.j)
  }

  fn refresh_if_needed(&mut self) {
    if self.j == 8 {
      self.i += 1;
      self.j = 0;
    }
  }

  fn byte(&self) -> QCompressResult<u8> {
    if self.i < self.bytes.len() {
      Ok(self.bytes[self.i])
    } else {
      Err(QCompressError::insufficient_data(
        "byte(): reached end of data available to BitReader"
      ))
    }
  }

  /// Returns a slice into the next `n` bytes. Will return an error if
  /// there are not enough bytes remaining in the reader or the reader is
  /// misaligned.
  pub fn read_aligned_bytes(&mut self, n: usize) -> QCompressResult<&[u8]> {
    self.refresh_if_needed();

    if self.j != 0 {
      Err(QCompressError::invalid_argument(format!(
        "cannot read aligned bytes on misaligned bit reader at byte {} bit {}",
        self.i,
        self.j,
      )))
    } else if self.i + n > self.bytes.len() {
      Err(QCompressError::insufficient_data(format!(
        "cannot read {} aligned bytes at byte {} out of {}",
        n,
        self.i,
        self.bytes.len(),
      )))
    } else {
      let res = &self.bytes[self.i..self.i + n];
      self.i += n - 1;
      self.j = 8;
      Ok(res)
    }
  }

  /// Returns the next bit. Will return an error if we have reached the end
  /// of the reader.
  pub fn read_one(&mut self) -> QCompressResult<bool> {
    self.refresh_if_needed();

    let res = bits::bit_from_byte(self.byte()?, self.j);
    self.j += 1;
    Ok(res)
  }

  /// Returns the next `n` bits. Will return an error if there are not
  /// enough bits remaining.
  pub fn read(&mut self, n: usize) -> QCompressResult<Vec<bool>> {
    let mut res = Vec::with_capacity(n);

    // implementation not well optimized because this is only used in reading header
    let mut byte = self.byte()?;
    for _ in 0..n {
      if self.j == 8 {
        self.i += 1;
        self.j = 0;
        byte = self.byte()?;
      }
      res.push(bits::bit_from_byte(byte, self.j));
      self.j += 1;
    }
    Ok(res)
  }

  pub(crate) fn read_diff<Diff: UnsignedLike>(&mut self, n: usize) -> QCompressResult<Diff> {
    if self.i * 8 + self.j + n > self.total_bits {
      return Err(QCompressError::insufficient_data(
        "read_diff(): reached end of data available to BitReader"
      ))
    }

    Ok(self.unchecked_read_diff::<Diff>(n))
  }

  pub(crate) fn read_usize(&mut self, n: usize) -> QCompressResult<usize> {
    Ok(self.read_diff::<u64>(n)? as usize)
  }

  // returns (bits read, idx)
  pub(crate) fn read_prefix_table_idx(&mut self) -> QCompressResult<(usize, usize)> {
    self.refresh_if_needed();

    let n_plus_j = PREFIX_TABLE_SIZE_LOG + self.j;
    if n_plus_j <= 8 {
      let shift = 8 - n_plus_j;
      let res = (self.byte()? & LEFT_MASKS[self.j] & RIGHT_MASKS[n_plus_j]) >> shift;
      self.j = n_plus_j;
      Ok((PREFIX_TABLE_SIZE_LOG, res as usize))
    } else {
      let remaining = n_plus_j - 8;
      let mut res = ((self.byte()? & LEFT_MASKS[self.j]) as usize) << remaining;
      self.i += 1;
      if self.i < self.bytes.len() {
        let shift = 8 - remaining;
        res |= ((self.unchecked_byte() & RIGHT_MASKS[remaining]) >> shift) as usize;
        self.j = remaining;
        Ok((PREFIX_TABLE_SIZE_LOG, res))
      } else {
        self.j = 0;
        Ok((PREFIX_TABLE_SIZE_LOG - remaining, res))
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

  fn unchecked_byte(&self) -> u8 {
    self.bytes[self.i]
  }

  /// Returns the next bit. Will panic if we have reached the end of the
  /// reader. This tends to be much faster than `read_one()`.
  pub fn unchecked_read_one(&mut self) -> bool {
    self.refresh_if_needed();

    let res = bits::bit_from_byte(self.unchecked_byte(), self.j);
    self.j += 1;
    res
  }

  pub(crate) fn unchecked_read_diff<Diff: UnsignedLike>(&mut self, n: usize) -> Diff {
    if n == 0 {
      return Diff::ZERO;
    }

    self.refresh_if_needed();

    let n_plus_j = n + self.j;
    if n_plus_j <= 8 {
      // it's all in the current byte
      let shift = 8 - n_plus_j;
      let res = Diff::from((self.bytes[self.i] & LEFT_MASKS[self.j] & RIGHT_MASKS[n_plus_j]) >> shift);
      self.j = n_plus_j;
      res
    } else {
      let mut res = Diff::ZERO;
      let mut remaining = n; // how many bits we still need to read
      // let mut s = 0;  // number of bits read into the u64 so far
      remaining -= 8 - self.j;
      res |= Diff::from(self.bytes[self.i] & LEFT_MASKS[self.j]) << remaining;
      while remaining >= 8 {
        self.i += 1;
        remaining -= 8;
        res |= Diff::from(self.bytes[self.i]) << remaining;
      }
      if remaining > 0 {
        self.i += 1;
        let shift = 8 - remaining;
        res |= Diff::from((self.bytes[self.i] & RIGHT_MASKS[remaining]) >> shift);
        self.j = remaining;
      } else {
        self.j = 8;
      }
      res
    }
  }

  pub(crate) fn unchecked_read_prefix_table_idx(&mut self) -> usize {
    self.refresh_if_needed();

    let n_plus_j = PREFIX_TABLE_SIZE_LOG + self.j;
    if n_plus_j <= 8 {
      let shift = 8 - n_plus_j;
      let res = (self.unchecked_byte() & LEFT_MASKS[self.j] & RIGHT_MASKS[n_plus_j]) >> shift;
      self.j = n_plus_j;
      res as usize
    } else {
      let remaining = n_plus_j - 8;
      let mut res = ((self.unchecked_byte() & LEFT_MASKS[self.j]) as usize) << remaining;
      self.i += 1;
      let shift = 8 - remaining;
      res |= ((self.unchecked_byte() & RIGHT_MASKS[remaining]) >> shift) as usize;
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
    if self.j > 0 {
      if self.byte()? & LEFT_MASKS[self.j] > 0 {
        return Err(f());
      }
      self.j = 8;
    }
    Ok(())
  }

  /// Skips forward `n` bits. Will NOT check whether
  /// the resulting position is in bounds or not.
  pub fn seek(&mut self, n: usize) {
    let forward_bit_idx = self.j + n;
    self.i += forward_bit_idx.div_euclid(8);
    self.j = forward_bit_idx.rem_euclid(8);
  }

  /// Skips backward `n` bits. Will panic if the resulting position is less
  /// than 0.
  pub fn rewind(&mut self, n: usize) {
    if n <= self.j {
      self.j -= n;
    } else {
      let backward_bit_idx = (n + 7) - self.j;
      self.i -= backward_bit_idx.div_euclid(8);
      self.j = 7 - backward_bit_idx.rem_euclid(8);
    }
  }
}

#[cfg(test)]
mod tests {
  use crate::BitReader;
  use crate::errors::QCompressResult;

  #[test]
  fn test_bit_reader() -> QCompressResult<()>{
    // bits: 1001 1010  0110 1011  0010 1101
    let bytes = vec![0x9a, 0x6b, 0x2d];
    let mut bit_reader = BitReader::from(bytes);
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
    let mut reader = BitReader::from(vec![0; 6]);
    reader.seek(43);

    reader.rewind(2);
    assert_eq!(reader.inds(), (5, 1));
    reader.rewind(2);
    assert_eq!(reader.inds(), (4, 7));
    reader.rewind(7);
    assert_eq!(reader.inds(), (4, 0));
    reader.rewind(8);
    assert_eq!(reader.inds(), (3, 0));
    reader.rewind(17);
    assert_eq!(reader.inds(), (0, 7));
  }
}
